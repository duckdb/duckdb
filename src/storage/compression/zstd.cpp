#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/compression/utils.hpp"
#include "zstd_wrapper.hpp"
#include "duckdb/storage/compression/zstd.hpp"
#include "duckdb/common/allocator.hpp"

#define ZDICT_STATIC_LINKING_ONLY /* for ZDICT_DICTSIZE_MIN */
#include "zdict.h"
#define ZSTD_STATIC_LINKING_ONLY /* for ZSTD_createCDict_byReference*/
#include "zstd.h"

/*
+--------------------------------------------+
|                Dictionary                  |
|   +------------------------------------+   |
|   |   uint32_t dictionary_size         |   |
|   |   void    *dictionary_buffer       |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
|            Vector Metadata                 |
|   +------------------------------------+   |
|   |   int64_t  page_id[]               |   |
|   |   uint32_t page_offset[]           |   |
|   |   uint64_t uncompressed_size[]     |   |
|   |   uint32_t compressed_size[]       |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
|              [Vector Data]+                |
|   +------------------------------------+   |
|   |   uint32_t lengths[]               |   |
|   |   void    *compressed_data         |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
*/

using dict_size_t = uint32_t;
using page_id_t = int64_t;
using page_offset_t = uint32_t;
using uncompressed_size_t = uint64_t;
using compressed_size_t = uint32_t;
using string_length_t = uint32_t;

static int32_t GetCompressionLevel() {
	return duckdb_zstd::ZSTD_defaultCLevel();
}

namespace duckdb {

//===--------------------------------------------------------------------===//
// (Sampling) Analyze
//===--------------------------------------------------------------------===//

bool ZSTDSamplingState::Finalize() {
	finalized = true;
	concatenated_samples = malloc(total_sample_size);
	if (!concatenated_samples) {
		return false;
	}
	idx_t offset = 0;

	sample_sizes.reserve(vector_sizes.size() * STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < vector_sizes.size(); i++) {
		auto &count = vector_sizes[i];
		auto &vec = to_sample_vectors[i];

		UnifiedVectorFormat vdata;
		vec.ToUnifiedFormat(count, vdata);

		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				continue;
			}
			auto &str = data[idx];
			auto string_size = str.GetSize();
			memcpy((char *)concatenated_samples + offset, str.GetData(), string_size);
			offset += string_size;
			sample_sizes.push_back(string_size);
		}
	}
	return true;
}

void ZSTDSamplingState::Reset() {
	free(concatenated_samples);
	concatenated_samples = nullptr;
	sample_sizes.clear();
	finalized = false;
	to_sample_vectors.clear();
	vector_sizes.clear();
	total_sample_size = 0;
	sampling_state = AnalyzeSamplingState();
}

struct ZSTDStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct ZSTDAnalyzeState : public AnalyzeState {
public:
	ZSTDAnalyzeState(CompressionInfo &info) : AnalyzeState(info), compression_dict(nullptr), context(nullptr) {
		context = duckdb_zstd::ZSTD_createCCtx();
	}
	~ZSTDAnalyzeState() {
		duckdb_zstd::ZSTD_freeCCtx(context);
		duckdb_zstd::ZSTD_freeCDict(compression_dict);
	}

public:
	inline void AppendString(const string_t &str) {
		auto string_size = str.GetSize();
		total_size += string_size;
	}
	idx_t GetVectorCount() {
		idx_t vector_count = count / STANDARD_VECTOR_SIZE;
		vector_count += (count % STANDARD_VECTOR_SIZE) != 0;
		return vector_count;
	}

	idx_t GetVectorMetadataSize() {
		auto vector_count = GetVectorCount();

		idx_t vector_metadata_size = 0;
		vector_metadata_size += sizeof(page_id_t);
		vector_metadata_size += sizeof(page_offset_t);
		vector_metadata_size += sizeof(uncompressed_size_t);
		vector_metadata_size += sizeof(compressed_size_t);
		return vector_count * vector_metadata_size;
	}

public:
	ZSTDSamplingState sampling_state;

	//! The trained 'dictBuffer' (populated in FinalAnalyze)
	DictBuffer dict;
	duckdb_zstd::ZSTD_CDict *compression_dict;

	duckdb_zstd::ZSTD_CCtx *context;
	idx_t total_size = 0;
	idx_t count = 0;
};

unique_ptr<AnalyzeState> ZSTDStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	return make_uniq<ZSTDAnalyzeState>(info);
}

// Determines wether compression is possible and calculates sizes for the FinalAnalyze
bool ZSTDStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		auto &str = data[idx];
		auto string_size = str.GetSize();
		state.total_size += string_size;
	}
	state.sampling_state.Sample(input, count);
	state.count += count;
	return true;
}

DictBuffer CreateDictFromSamples(ZSTDAnalyzeState &state) {
	auto &sampling_state = state.sampling_state;
	if (!sampling_state.Finalize()) {
		return DictBuffer();
	}

	auto space_required = state.GetVectorMetadataSize();
	space_required += sizeof(block_id_t);
	if (space_required >= state.info.GetBlockSize()) {
		// FIXME: allow vector metadata to spill over to the next page
		return DictBuffer();
	}
	auto remaining_size = state.info.GetBlockSize() - space_required;
	if (remaining_size < ZDICT_DICTSIZE_MIN) {
		// Not enough room to fit the metadata + the dictionary on the page
		return DictBuffer();
	}

	idx_t dict_buffer_size = MaxValue<idx_t>(sampling_state.total_sample_size / 100, ZDICT_DICTSIZE_MIN);
	if (dict_buffer_size > remaining_size) {
		// FIXME: perhaps we want to spill this to a separate page instead then?
		dict_buffer_size = remaining_size;
	}

	DictBuffer buffer(UnsafeNumericCast<dict_size_t>(dict_buffer_size));
	if (!buffer.Buffer()) {
		return DictBuffer();
	}

	auto res = duckdb_zstd::ZDICT_trainFromBuffer(
	    buffer.Buffer(), buffer.Capacity(), sampling_state.concatenated_samples,
	    (size_t *)sampling_state.sample_sizes.data(), UnsafeNumericCast<uint32_t>(sampling_state.sample_sizes.size()));
	if (duckdb_zstd::ZSTD_isError(res)) {
		return DictBuffer();
	}
	buffer.SetSize(UnsafeNumericCast<dict_size_t>(res));
	return buffer;
}

// Compression score to determine which compression to use
idx_t ZSTDStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();

	if (state.count < 10) {
		// Not enough samples to train a dictionary
		return DConstants::INVALID_INDEX;
	}

	state.dict = CreateDictFromSamples(state);
	if (!state.dict) {
		// Training the dictionary failed for some reason
		return NumericLimits<idx_t>::Maximum();
	}

	state.compression_dict =
	    duckdb_zstd::ZSTD_createCDict_byReference(state.dict.Buffer(), state.dict.Size(), GetCompressionLevel());

	auto required_space = duckdb_zstd::ZSTD_compressBound(state.sampling_state.total_sample_size);
	auto dst = malloc(required_space);
	// FIXME: use streaming compression ? both to simulate the real compression better and to reduce the amount of space
	// we have to allocate here
	auto compressed_size = duckdb_zstd::ZSTD_compress_usingCDict(
	    state.context, dst, required_space, state.sampling_state.concatenated_samples,
	    state.sampling_state.total_sample_size, state.compression_dict);
	free(dst);
	if (duckdb_zstd::ZSTD_isError(compressed_size)) {
		return NumericLimits<idx_t>::Maximum();
	}

	double compression_ratio = state.sampling_state.total_sample_size / compressed_size;
	// Check what the size of the data would be if all of it would be compressed at this compression ratio.
	idx_t expected_compressed_size = LossyNumericCast<idx_t>(state.total_size / compression_ratio);
	state.sampling_state.Reset();

	idx_t estimated_size = 0;
	estimated_size += state.dict.Size();
	estimated_size += expected_compressed_size;

	estimated_size += state.count * sizeof(string_length_t);
	estimated_size += state.GetVectorMetadataSize();

	// we only use zstd if it is at least 1.3 times better than the alternative
	auto zstd_penalty_factor = 1.3;

	return LossyNumericCast<idx_t>(estimated_size * zstd_penalty_factor);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

class ZSTDCompressionState : public CompressionState {
public:
	static constexpr int COMPRESSION_LEVEL = 3;

	explicit ZSTDCompressionState(ColumnDataCheckpointer &checkpointer, unique_ptr<ZSTDAnalyzeState> &&analyze_state_p)
	    : CompressionState(analyze_state_p->info), analyze_state(std::move(analyze_state_p)),
	      checkpointer(checkpointer), function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ZSTD)) {
		CreateSegment(checkpointer.GetRowGroup().start);
		SetCurrentBuffer(segment_handle);
		vector_count = 0;
		tuple_count = 0;

		auto &dict = analyze_state->dict;
		// Write the dictionary to the start of the segment
		D_ASSERT(dict.Size() <= (info.GetBlockSize() - sizeof(block_id_t) - sizeof(dict_size_t)));
		Store<dict_size_t>(dict.Size(), current_buffer_ptr);
		current_buffer_ptr += sizeof(dict_size_t);
		memcpy(current_buffer_ptr, dict.Buffer(), dict.Size());
		current_buffer_ptr += dict.Size();

		total_vector_count = analyze_state->GetVectorCount();

		// Set pointers to the Vector Metadata
		idx_t offset = GetCurrentOffset();

		offset = AlignValue<idx_t, sizeof(page_id_t)>(offset);
		page_ids = (page_id_t *)(current_buffer->Ptr() + offset);
		offset += (sizeof(page_id_t) * total_vector_count);

		offset = AlignValue<idx_t, sizeof(page_offset_t)>(offset);
		page_offsets = (page_offset_t *)(current_buffer->Ptr() + offset);
		offset += (sizeof(page_offset_t) * total_vector_count);

		offset = AlignValue<idx_t, sizeof(uncompressed_size_t)>(offset);
		uncompressed_sizes = (uncompressed_size_t *)(current_buffer->Ptr() + offset);
		offset += (sizeof(uncompressed_size_t) * total_vector_count);

		offset = AlignValue<idx_t, sizeof(compressed_size_t)>(offset);
		compressed_sizes = (compressed_size_t *)(current_buffer->Ptr() + offset);
		offset += (sizeof(compressed_size_t) * total_vector_count);

		current_buffer_ptr = current_buffer->Ptr() + offset;
	}

public:
	void SetCurrentBuffer(BufferHandle &handle) {
		current_buffer = &handle;
		current_buffer_ptr = handle.Ptr();
	}

	void InitializeVector() {
		if (vector_count + 1 >= total_vector_count) {
			vector_size - analyze_state->count - (STANDARD_VECTOR_SIZE * vector_count);
		} else {
			vector_size = STANDARD_VECTOR_SIZE;
		}
		auto current_offset = GetCurrentOffset();
		// FIXME: align the `current_offset` ??
		starting_offset = current_offset;
		starting_page = GetCurrentId();
		compressed_size = 0;
		uncompressed_size = 0;

		string_lengths = (string_length_t *)(current_buffer->Ptr() + current_offset);
		current_buffer_ptr = (data_ptr_t)string_lengths;
		current_buffer_ptr += vector_size * sizeof(string_length_t);

		duckdb_zstd::ZSTD_CCtx_reset(analyze_state->context, duckdb_zstd::ZSTD_reset_session_only);
		duckdb_zstd::ZSTD_CCtx_refCDict(analyze_state->context, analyze_state->compression_dict);
		duckdb_zstd::ZSTD_CCtx_setParameter(analyze_state->context, duckdb_zstd::ZSTD_c_compressionLevel,
		                                    GetCompressionLevel());
	}

	// void WriteToCompressionBuffer(const string_t &string) {
	//	auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
	//	auto required_space = string.GetSize() + compression_buffer_offset;
	//	auto current_size = compression_buffer ? compression_buffer.GetSize() : 0;
	//	required_space = MaxValue<idx_t>(info.GetBlockSize(), required_space);
	//	if (required_space >= current_size) {
	//		idx_t new_size = NextPowerOfTwo(required_space);
	//		auto old_buffer = std::move(compression_buffer);

	//		compression_buffer = buffer_manager.GetBufferAllocator().Allocate(new_size);
	//		if (old_buffer) {
	//			memcpy(compression_buffer.get(), old_buffer.get(), compression_buffer_offset);
	//		}
	//	}
	//	memcpy(compression_buffer.get() + compression_buffer_offset, string.GetData(), string.GetSize());
	//	compression_buffer_offset += string.GetSize();
	//}

	void CompressString(const string_t &string, bool end_of_vector) {
		duckdb_zstd::ZSTD_inBuffer in_buffer = {/*data = */ string.GetData(),
		                                        /*length = */ string.GetSize(),
		                                        /*pos = */ 0};

		uncompressed_size += string.GetSize();
		const auto end_mode = end_of_vector ? duckdb_zstd::ZSTD_e_end : duckdb_zstd::ZSTD_e_flush;

		size_t compress_result;
		while (true) {
			idx_t old_pos = out_buffer.pos;

			compress_result =
			    duckdb_zstd::ZSTD_compressStream2(analyze_state->context, &out_buffer, &in_buffer, end_mode);
			D_ASSERT(out_buffer.pos >= old_pos);
			auto diff = out_buffer.pos - old_pos;
			compressed_size += diff;

			if (duckdb_zstd::ZSTD_isError(compress_result)) {
				throw InvalidInputException("ZSTD Compression failed: %s",
				                            duckdb_zstd::ZSTD_getErrorName(compress_result));
			}
			if (compress_result == 0) {
				// Finished
				break;
			}
			// FIXME: will 'pos' in the out_buffer always reach 'size' or could the library refuse to fully utilize the
			// buffer ?? if that is the case, we should somehow serialize how many bytes are utilized for each page
			D_ASSERT(out_buffer.pos == out_buffer.size);
			// Allocate a new page
			FlushPage();
		}
	}

	void AddString(const string_t &string) {
		if (!tuple_count) {
			InitializeVector();
		}

		string_lengths[tuple_count] = UnsafeNumericCast<string_length_t>(string.GetSize());
		// WriteToCompressionBuffer(string);
		bool final_tuple = tuple_count + 1 >= vector_size;
		CompressString(string, final_tuple);

		tuple_count++;
		if (tuple_count == vector_size) {
			// Reached the end of this vector
			FlushVector();
		}
	}

	void FlushPage() {
		// Allocate a new block, write the new block id in the old block
		// flush the old block
		// update the pointers and 'current_buffer_ptr' etc to point to the new block
		// update the 'out_buffer'
	}

	void FlushVector() {
		// Write the metadata for this Vector
		page_ids[vector_count] = starting_page;
		page_offsets[vector_count] = starting_offset;
		compressed_sizes[vector_count] = compressed_size;
		uncompressed_sizes[vector_count] = uncompressed_size;
		vector_count++;

		tuple_count = 0;
	}

	page_id_t GetCurrentId() {
		if (&segment_handle == current_buffer.get()) {
			return INVALID_BLOCK;
		}
		D_ASSERT(data_block);
		return data_block->BlockId();
	}

	page_offset_t GetCurrentOffset() {
		auto &handle = *current_buffer;
		auto start_of_buffer = handle.Ptr();
		D_ASSERT(current_buffer_ptr >= start_of_buffer);
		return (page_offset_t)(current_buffer_ptr - start_of_buffer);
	}

	void CreateSegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		segment = std::move(compressed_segment);
		segment->function = function;

		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		segment_handle = buffer_manager.Pin(segment->block);
	}

	void Finalize() {
		D_ASSERT(!tuple_count);

		auto &state = checkpointer.GetCheckpointState();
		idx_t segment_block_size;
		if (current_buffer.get() == &segment_handle) {
			segment_block_size = GetCurrentOffset();
		} else {
			// If we have overflowed to other pages, the segment block will have been fully utilized
			// FIXME: we might leave some data at the end uninitialized ???
			segment_block_size = info.GetBlockSize();
		}
		state.FlushSegment(std::move(segment), segment_block_size);
		segment.reset();
	}

	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t compressed_string_len) {
		throw InternalException("FIXME: ZSTD UpdateState");
	}

	void AddNull() {
		// TODO: fix
		AddString("");
	}

	void AddEmptyString() {
		AddNull();
		// UncompressedStringStorage::UpdateStringStats(segment->stats, ""); //?
	}

	// size_t GetRequiredSize(size_t string_len) {
	// 	throw InternalException("FIXME: ZSTD GetRequiredSize");
	// 	return 0;
	// }

	// Checks if there is enough space, if there is, sets last_fitting_size
	// bool HasEnoughSpace(size_t string_len) {
	// 	throw InternalException("FIXME: ZSTD HasEnoughSpace");
	// 	return false;
	// }

public:
	unique_ptr<ZSTDAnalyzeState> analyze_state;
	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// The segment state
	unique_ptr<ColumnSegment> segment;
	BufferHandle segment_handle;

	// non-segment block
	shared_ptr<BlockHandle> data_block;
	BufferHandle data_buffer;

	// Current block state
	optional_ptr<BufferHandle> current_buffer;
	data_ptr_t current_buffer_ptr;

	//===--------------------------------------------------------------------===//
	// Vector metadata
	//===--------------------------------------------------------------------===//
	page_id_t starting_page;
	page_offset_t starting_offset;

	page_id_t *page_ids;
	page_offset_t *page_offsets;
	uncompressed_size_t *uncompressed_sizes;
	compressed_size_t *compressed_sizes;
	//! The amount of vectors we've seen so far
	idx_t vector_count = 0;
	//! The amount of vectors we're writing
	idx_t total_vector_count = 0;
	//! The compression context indicating where we are in the output buffer
	duckdb_zstd::ZSTD_outBuffer out_buffer;
	idx_t uncompressed_size = 0;
	idx_t compressed_size = 0;
	string_length_t *string_lengths;

	//! Amount of tuples we have seen for the current vector
	idx_t tuple_count = 0;
	//! The expected size of this vector (STANDARD_VECTOR_SIZE except for the last one)
	idx_t vector_size;
};

unique_ptr<CompressionState> ZSTDStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> analyze_state_p) {
	return make_uniq<ZSTDCompressionState>(checkpointer,
	                                       unique_ptr_cast<AnalyzeState, ZSTDAnalyzeState>(std::move(analyze_state_p)));
}

void ZSTDStorage::Compress(CompressionState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<ZSTDCompressionState>();

	// Get vector data
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		// Note: we treat nulls and empty strings the same
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			state.AddNull();
			continue;
		}
		state.AddString(data[idx]);
	}
	state.segment->count += count;
}

void ZSTDStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<ZSTDCompressionState>();
	state.Finalize();
}

struct ZSTDVectorScanMetadata {
	block_id_t block_id;
	page_offset_t block_offset;

	uncompressed_size_t uncompressed_size;
	compressed_size_t compressed_size;

	//! The amount of tuples that are in this Vector
	idx_t count;
	//! The current offset in this Vector
	idx_t offset;
};

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct ZSTDScanState : public SegmentScanState {
public:
	ZSTDScanState() : decompression_dict(nullptr) {
		decompression_context = duckdb_zstd::ZSTD_createDCtx();
	}
	~ZSTDScanState() {
		duckdb_zstd::ZSTD_freeDDict(decompression_dict);
		duckdb_zstd::ZSTD_freeDCtx(decompression_context);
	}

public:
	ZSTDVectorScanMetadata GetVectorMetadata(idx_t start_idx) {
		idx_t entire_vectors = start_idx / STANDARD_VECTOR_SIZE;
		auto vector_idx = entire_vectors + ((start_idx % STANDARD_VECTOR_SIZE) != 0);
		idx_t remaining = MinValue<idx_t>(segment_count - start_idx, STANDARD_VECTOR_SIZE);
		auto offset = start_idx - (entire_vectors * STANDARD_VECTOR_SIZE);

		return ZSTDVectorScanMetadata {
		    /* block_id = */ page_ids[vector_idx],
		    /* block_offset = */ page_offsets[vector_idx],
		    /* uncompressed_size = */ uncompressed_sizes[vector_idx],
		    /* compressed_size = */ compressed_sizes[vector_idx],
		    /* count = */ remaining,
		    /* offset = */ offset,
		};
	}

	void ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count) {
		D_ASSERT(offset + count <= STANDARD_VECTOR_SIZE);

		auto vector_metadata = GetVectorMetadata(start_idx);

		scanned_count += count;
	}

public:
	DictBuffer dict;

	duckdb_zstd::ZSTD_DCtx *decompression_context = 0;
	duckdb_zstd::ZSTD_DDict *decompression_dict = 0;

	//! non-segment block that is being read from
	shared_ptr<BlockHandle> data_block;
	BufferHandle data_buffer;

	BufferHandle handle;
	data_ptr_t current_data_ptr;
	//! The temporary buffer to store decompressed data
	AllocatedData decompression_buffer;
	//! The amount of tuples we've already consumed from this Vector
	idx_t offset = 0;

	//===--------------------------------------------------------------------===//
	// Vector metadata
	//===--------------------------------------------------------------------===//
	page_id_t *page_ids;
	page_offset_t *page_offsets;
	uncompressed_size_t *uncompressed_sizes;
	compressed_size_t *compressed_sizes;

	//! The amount of tuples stored in the segment
	idx_t segment_count;
	//! The amount of tuples consumed
	idx_t scanned_count;
};

unique_ptr<SegmentScanState> ZSTDStorage::StringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<ZSTDScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);

	// load dictionary
	auto data = result->handle.Ptr() + segment.GetBlockOffset();
	auto dict_size = Load<dict_size_t>(data);

	result->dict = DictBuffer(data + sizeof(dict_size_t), dict_size);

	idx_t offset = 0;
	offset += sizeof(dict_size_t);
	result->decompression_dict = duckdb_zstd::ZSTD_createDDict_byReference(result->dict.Buffer(), result->dict.Size());
	offset += result->dict.Size();

	result->segment_count = segment.count.load();
	idx_t segment_count = result->segment_count;
	idx_t amount_of_vectors = (segment_count / STANDARD_VECTOR_SIZE) + ((segment_count % STANDARD_VECTOR_SIZE) != 0);

	// Set pointers to the Vector Metadata

	offset = AlignValue<idx_t, sizeof(page_id_t)>(offset);
	result->page_ids = (page_id_t *)(data + offset);
	offset += (sizeof(page_id_t) * amount_of_vectors);

	offset = AlignValue<idx_t, sizeof(page_offset_t)>(offset);
	result->page_offsets = (page_offset_t *)(data + offset);
	offset += (sizeof(page_offset_t) * amount_of_vectors);

	offset = AlignValue<idx_t, sizeof(uncompressed_size_t)>(offset);
	result->uncompressed_sizes = (uncompressed_size_t *)(data + offset);
	offset += (sizeof(uncompressed_size_t) * amount_of_vectors);

	offset = AlignValue<idx_t, sizeof(compressed_size_t)>(offset);
	result->compressed_sizes = (compressed_size_t *)(data + offset);
	offset += (sizeof(compressed_size_t) * amount_of_vectors);

	result->current_data_ptr = data + offset;
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	auto &scan_state = state.scan_state->template Cast<ZSTDScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	// FIXME: this could read over a boundary of our internal vector groups
	// in which case we would need to read the remainder of the last group

	// We should have a fast path that directly reads into the StringHeap of the 'result' if an entire Vector is
	// requested and it aligns with our own internal Vector groups
	scan_state.ScanPartial(start, result, result_offset, scan_count);
}

void ZSTDStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// StringScanPartial(segment, state, scan_count, result, 0);

	auto &scan_state = state.scan_state->Cast<ZSTDScanState>();
	auto &block_manager = segment.GetBlockManager();
	auto &buffer_manager = block_manager.buffer_manager;

	data_ptr_t src = scan_state.current_data_ptr;
	auto result_data = FlatVector::GetData<string_t>(result);

	duckdb_zstd::ZSTD_DCtx *zstd_context = duckdb_zstd::ZSTD_createDCtx();

	//// create temporary buffer
	//// TODO: fix this
	// char buffer[1024];

	// for (idx_t i = 0; i < scan_count; i++) {

	//	// get metadata
	//	string_metadata_t *meta = reinterpret_cast<string_metadata_t *>(src);
	//	size_t uncompressed_size = duckdb_zstd::ZSTD_decompress_usingDDict(
	//	    zstd_context, buffer, 1024, src + sizeof(string_metadata_t), meta->size, scan_state.decompression_dict);

	//	// ALLOCATE STRING?
	//	result_data[i] = string_t(buffer, UnsafeNumericCast<uint32_t>(uncompressed_size));
	//}

	duckdb_zstd::ZSTD_freeDCtx(zstd_context);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {
	throw InternalException("FIXME: ZSTD StringFetchRow");
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ZSTDFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(CompressionType::COMPRESSION_ZSTD, data_type, ZSTDStorage::StringInitAnalyze,
	                           ZSTDStorage::StringAnalyze, ZSTDStorage::StringFinalAnalyze,
	                           ZSTDStorage::InitCompression, ZSTDStorage::Compress, ZSTDStorage::FinalizeCompress,
	                           ZSTDStorage::StringInitScan, ZSTDStorage::StringScan, ZSTDStorage::StringScanPartial,
	                           ZSTDStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool ZSTDFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

} // namespace duckdb
