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

	state.compression_dict = duckdb_zstd::ZSTD_createCDict_byReference(state.dict.Buffer(), state.dict.Size(),
	                                                                   duckdb_zstd::ZSTD_defaultCLevel());

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

		auto &dict = analyze_state->dict;
		// Write the dictionary to the start of the segment
		D_ASSERT(dict.Size() <= (info.GetBlockSize() - sizeof(block_id_t) - sizeof(dict_size_t)));
		Store<dict_size_t>(dict.Size(), current_buffer_ptr);
		current_buffer_ptr += sizeof(dict_size_t);
		memcpy(current_buffer_ptr, dict.Buffer(), dict.Size());
		current_buffer_ptr += dict.Size();

		idx_t amount_of_vectors = analyze_state->GetVectorCount();

		// Set pointers to the Vector Metadata
		page_ids = (page_id_t *)current_buffer_ptr;
		current_buffer_ptr += sizeof(page_id_t) * amount_of_vectors;

		page_offsets = (page_offset_t *)current_buffer_ptr;
		current_buffer_ptr += sizeof(page_offset_t) * amount_of_vectors;

		uncompressed_sizes = (uncompressed_size_t *)current_buffer_ptr;
		current_buffer_ptr += sizeof(uncompressed_size_t) * amount_of_vectors;

		compressed_sizes = (compressed_size_t *)current_buffer_ptr;
		current_buffer_ptr += sizeof(compressed_size_t) * amount_of_vectors;
	}

public:
	void SetCurrentBuffer(BufferHandle &handle) {
		current_buffer = &handle;
		current_buffer_ptr = handle.Ptr();
	}

	void AddString(const string_t &string) {
		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		auto required_space = string.GetSize() + compression_buffer_offset;
		auto current_size = compression_buffer ? compression_buffer.GetSize() : 0;
		required_space = MaxValue<idx_t>(info.GetBlockSize(), required_space);
		if (required_space >= current_size) {
			idx_t new_size = NextPowerOfTwo(required_space);
			auto old_buffer = std::move(compression_buffer);

			compression_buffer = buffer_manager.GetBufferAllocator().Allocate(new_size);
			if (old_buffer) {
				memcpy(compression_buffer.get(), old_buffer.get(), compression_buffer_offset);
			}
		}
		memcpy(compression_buffer.get() + compression_buffer_offset, string.GetData(), string.GetSize());
		compression_buffer_offset += string.GetSize();
		string_lengths[buffered_count] = UnsafeNumericCast<string_length_t>(string.GetSize());
		buffered_count++;
		if (buffered_count == STANDARD_VECTOR_SIZE) {
			FlushVector();
		}
	}

	void FlushVector() {
		// Write a Vector worth of strings to storage

		page_id_t starting_page_id = GetCurrentId();
		page_offset_t starting_page_offset = GetCurrentOffset();

		// compress the data to 'storage_buffer', then figure out if it can fit in the current segment or not
		// also write the lengths of every string
		auto compressed_size = duckdb_zstd::ZSTD_compress_usingCDict(
		    /* context = */ analyze_state->context,
		    /* dst = */ storage_buffer.get(),
		    /* dstCapacity = */ storage_buffer.GetSize(),
		    /* src = */ compression_buffer.get(),
		    /* srcSize = */ compression_buffer_offset,
		    /* dict = */ analyze_state->compression_dict);
		if (duckdb_zstd::ZSTD_isError(compressed_size)) {
			throw InvalidInputException("ZSTD Compression failed: %s", duckdb_zstd::ZSTD_getErrorName(compressed_size));
		}

		// TODO:
		// Write from the storage_buffer into the block
		// FIXME: use streaming compression to skip the 'storage_buffer' entirely

		// Write the metadata for this Vector
		page_ids[vector_count] = starting_page_id;
		page_offsets[vector_count] = starting_page_offset;
		compressed_sizes[vector_count] = UnsafeNumericCast<compressed_size_t>(compressed_size);
		uncompressed_sizes[vector_count] = compression_buffer_offset;
		vector_count++;

		compression_buffer_offset = 0;
		buffered_count = 0;
	}

	data_ptr_t GetStorageBuffer() {
		if (!storage_buffer) {
			auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
			storage_buffer = buffer_manager.GetBufferAllocator().Allocate(info.GetBlockSize() * 2);
		}
		return storage_buffer.get();
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
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(segment), analyze_state->count);
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
	page_id_t *page_ids;
	page_offset_t *page_offsets;
	uncompressed_size_t *uncompressed_sizes;
	compressed_size_t *compressed_sizes;
	//! The amount of vectors we've seen so far
	idx_t vector_count;

	//===--------------------------------------------------------------------===//
	// Temporary buffers
	//===--------------------------------------------------------------------===//

	//! The temporary buffer to store the lengths before compressing+serializing them
	uint32_t string_lengths[STANDARD_VECTOR_SIZE];

	//! Temporary buffer to store the compressed data before writing
	//! This is necessary when we will cross block boundaries
	AllocatedData storage_buffer;

	//! The buffer where strings are copied to before compressing
	AllocatedData compression_buffer;
	idx_t compression_buffer_offset = 0;
	//! Amount of tuples we are currently buffering to compress
	idx_t buffered_count = 0;
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

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct ZSTDScanState : public SegmentScanState {
public:
	ZSTDScanState() : decompression_dict(nullptr) {
	}
	~ZSTDScanState() {
		duckdb_zstd::ZSTD_freeDDict(decompression_dict);
	}

public:
	BufferHandle handle;
	duckdb_zstd::ZSTD_DCtx *decompression_context;
	duckdb_zstd::ZSTD_DDict *decompression_dict;
	data_ptr_t current_data_ptr;
};

unique_ptr<SegmentScanState> ZSTDStorage::StringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<ZSTDScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);

	// load dictionary
	auto data = result->handle.Ptr() + segment.GetBlockOffset();
	auto dict_size = Load<dict_size_t>(data);
	data += sizeof(dict_size_t);

	result->decompression_dict = duckdb_zstd::ZSTD_createDDict_byReference(data, dict_size);
	data += dict_size;
	result->current_data_ptr = data;

	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	// auto &scan_state = state.scan_state->template Cast<ZSTDScanState>();
	// auto start = segment.GetRelativeIndex(state.row_index);

	// auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	// auto source_data = data + start * sizeof(T);

	// result.SetVectorType(VectorType::FLAT_VECTOR);
	// FlatVector::SetData(result, source_data);
	throw InternalException("FIXME: ZSTD StringScanPartial");
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
