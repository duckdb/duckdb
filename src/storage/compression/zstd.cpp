#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/constants.hpp"
#include "zstd_wrapper.hpp"
#include "duckdb/storage/compression/zstd.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

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
|   |   uint64_t compressed_size[]       |   |
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
using compressed_size_t = uint64_t;
using string_length_t = uint32_t;

static int32_t GetCompressionLevel() {
	return duckdb_zstd::ZSTD_defaultCLevel();
}

static constexpr idx_t ZSTD_VECTOR_SIZE = STANDARD_VECTOR_SIZE > 2048 ? STANDARD_VECTOR_SIZE : 2048;
//! The combined string length of a vector required for ZSTD to be effective (1Kb)
static constexpr idx_t MINIMUM_AVERAGE_VECTOR_STRING_LENGTH = 1 << 10;
//! The minimum size for a string to be used as a sample
static constexpr idx_t ZSTD_SAMPLE_MIN_SIZE = 8;
//! The minimum amount of samples needed to train a dictionary
static constexpr idx_t ZSTD_MIN_SAMPLE_COUNT = 10;

namespace duckdb {

struct ZSTDStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static void PrepareCompress(AnalyzeState &state_p, Vector &input, idx_t count);
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

	// Segment state metadata
	// Required because we are creating additional pages that have to be cleaned up

	static unique_ptr<CompressedSegmentState> StringInitSegment(ColumnSegment &segment, block_id_t block_id,
	                                                            optional_ptr<ColumnSegmentState> segment_state);
	static unique_ptr<ColumnSegmentState> SerializeState(ColumnSegment &segment);
	static unique_ptr<ColumnSegmentState> DeserializeState(Deserializer &deserializer);
	static void CleanupState(ColumnSegment &segment);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//

struct ZSTDPrepareState {
public:
	ZSTDPrepareState() {
	}

public:
	AllocatedData sample_buffer;
	//! Whether all strings of a vector should be combined into a single sample
	bool combine_vectors = false;
	vector<idx_t> sample_sizes;
	//! The size that we have determined can be utilized by the dictionary buffer
	idx_t dict_size = 0;
	//! The dictionary trained from the samples
	DictBuffer dict;
	//! The current offset into the sample_buffer;
	idx_t sample_buffer_offset = 0;
	//! The amount of values already processed by prepare compress
	idx_t prepare_count = 0;
	bool finished = false;
};

struct ZSTDAnalyzeState : public AnalyzeState {
public:
	ZSTDAnalyzeState(CompressionInfo &info, DBConfig &config)
	    : AnalyzeState(info), config(config), compression_dict(nullptr), context(nullptr) {
		context = duckdb_zstd::ZSTD_createCCtx();
	}
	~ZSTDAnalyzeState() override {
		duckdb_zstd::ZSTD_freeCCtx(context);
		duckdb_zstd::ZSTD_freeCDict(compression_dict);
	}

public:
	inline void AppendString(const string_t &str) {
		auto string_size = str.GetSize();
		total_size += string_size;
	}
	inline idx_t GetVectorCount() const {
		idx_t vector_count = count / ZSTD_VECTOR_SIZE;
		vector_count += (count % ZSTD_VECTOR_SIZE) != 0;
		return vector_count;
	}

	inline idx_t GetVectorMetadataSize() const {
		auto vector_count = GetVectorCount();

		idx_t vector_metadata_size = 0;
		vector_metadata_size += sizeof(page_id_t) * vector_count;

		vector_metadata_size = AlignValue<idx_t, sizeof(page_offset_t)>(vector_metadata_size);
		vector_metadata_size += sizeof(page_offset_t) * vector_count;

		vector_metadata_size = AlignValue<idx_t, sizeof(uncompressed_size_t)>(vector_metadata_size);
		vector_metadata_size += sizeof(uncompressed_size_t) * vector_count;

		vector_metadata_size = AlignValue<idx_t, sizeof(compressed_size_t)>(vector_metadata_size);
		vector_metadata_size += sizeof(compressed_size_t) * vector_count;
		return vector_metadata_size;
	}

public:
	DBConfig &config;

	//! The trained 'dictBuffer' (populated in FinalAnalyze)
	DictBuffer dict;
	duckdb_zstd::ZSTD_CDict *compression_dict;

	duckdb_zstd::ZSTD_CCtx *context;
	//! The combined string lengths for all values in the segment
	idx_t total_size = 0;
	//! The total amount of values in the segment
	idx_t count = 0;
	ZSTDPrepareState prepare_state;
};

unique_ptr<AnalyzeState> ZSTDStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	auto &data_table_info = col_data.info;
	auto &attached_db = data_table_info.GetDB();
	auto &config = DBConfig::Get(attached_db);

	return make_uniq<ZSTDAnalyzeState>(info, config);
}

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
	state.count += count;
	return true;
}

optional_idx GetMaximumDictSize(ZSTDAnalyzeState &state) {
	auto space_required = state.GetVectorMetadataSize();
	space_required += sizeof(block_id_t);
	space_required += sizeof(dict_size_t);
	if (space_required >= state.info.GetBlockSize()) {
		return optional_idx();
	}
	space_required = AlignValue<idx_t>(space_required);
	auto remaining_size = state.info.GetBlockSize() - space_required;
	if (remaining_size < ZDICT_DICTSIZE_MIN) {
		// Not enough room to fit the metadata + the dictionary on the page
		return optional_idx();
	}

	if (state.count < ZSTD_MIN_SAMPLE_COUNT) {
		// Not enough samples to train a dictionary
		return optional_idx();
	}

	idx_t dict_buffer_size = MaxValue<idx_t>(state.total_size / 100, ZDICT_DICTSIZE_MIN);
	if (dict_buffer_size > remaining_size) {
		// FIXME: perhaps we want to spill this to a separate page instead then?
		dict_buffer_size = remaining_size;
	}
	if (state.config.options.limit_zstd_dictionary) {
		// Create the smallest possible dictionary to produce very badly compressed data
		// useful for testing how big amounts of compressed data behaves when it crosses page boundaries
		dict_buffer_size = ZDICT_DICTSIZE_MIN;
	}
	return dict_buffer_size;
}

DictBuffer CreateDictFromSamples(idx_t dict_buffer_size, const vector<idx_t> &sample_sizes,
                                 AllocatedData &sample_buffer) {
	DictBuffer buffer(UnsafeNumericCast<dict_size_t>(dict_buffer_size));
	if (!buffer.Buffer()) {
		return DictBuffer();
	}

	auto res = duckdb_zstd::ZDICT_trainFromBuffer(buffer.Buffer(), buffer.Capacity(), sample_buffer.get(),
	                                              (size_t *)sample_sizes.data(),
	                                              UnsafeNumericCast<uint32_t>(sample_sizes.size()));
	if (duckdb_zstd::ZSTD_isError(res)) {
		return DictBuffer();
	}
	buffer.SetSize(UnsafeNumericCast<dict_size_t>(res));
	return buffer;
}

// Compression score to determine which compression to use
idx_t ZSTDStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();

	if ((state.total_size / state.count) * ZSTD_VECTOR_SIZE < MINIMUM_AVERAGE_VECTOR_STRING_LENGTH) {
		// Compressing this with ZSTD won't be effective as the block sizes are too small
		return NumericLimits<idx_t>::Maximum();
	}
	if (state.count < ZSTD_MIN_SAMPLE_COUNT) {
		// Not enough values to train a dictionary
		return NumericLimits<idx_t>::Maximum();
	}

	auto required_size = GetMaximumDictSize(state);
	if (!required_size.IsValid()) {
		return NumericLimits<idx_t>::Maximum();
	}
	state.prepare_state.dict_size = required_size.GetIndex();

	auto expected_compressed_size = (double)state.total_size / 2.0;

	idx_t estimated_size = 0;
	estimated_size += MinValue<idx_t>(
	    state.total_size / 100, required_size.GetIndex()); // expected dictionary size is 100x smaller than the input
	estimated_size += LossyNumericCast<idx_t>(expected_compressed_size);

	estimated_size += state.count * sizeof(string_length_t);
	estimated_size += state.GetVectorMetadataSize();

	// we only use zstd if it is at least 1.3 times better than the alternative
	auto zstd_penalty_factor = 1.3;

	return LossyNumericCast<idx_t>((double)estimated_size * zstd_penalty_factor);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

class ZSTDCompressionState : public CompressionState {
public:
	static constexpr int COMPRESSION_LEVEL = 3;

	explicit ZSTDCompressionState(ColumnDataCheckpointer &checkpointer, unique_ptr<ZSTDAnalyzeState> &&analyze_state_p)
	    : CompressionState(analyze_state_p->info), analyze_state(std::move(analyze_state_p)),
	      checkpointer(checkpointer), partial_block_manager(checkpointer.GetCheckpointState().GetPartialBlockManager()),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ZSTD)) {
		CreateSegment(checkpointer.GetRowGroup().start);
		SetCurrentBuffer(segment_handle);
		vector_count = 0;
		tuple_count = 0;

		auto &dict = analyze_state->prepare_state.dict;
		// Write the dictionary to the start of the segment
		D_ASSERT(dict.Size() <= (info.GetBlockSize() - sizeof(block_id_t) - sizeof(dict_size_t) -
		                         analyze_state->GetVectorMetadataSize()));
		Store<dict_size_t>(dict.Size(), current_buffer_ptr);
		current_buffer_ptr += sizeof(dict_size_t);
		memcpy(current_buffer_ptr, dict.Buffer(), dict.Size());
		current_buffer_ptr += dict.Size();

		total_vector_count = analyze_state->GetVectorCount();

		// Set pointers to the Vector Metadata
		idx_t offset = GetCurrentOffset();

		offset = AlignValue<idx_t, sizeof(page_id_t)>(offset);
#ifdef DEBUG
		auto start_of_vector_metadata = current_buffer->Ptr() + offset;
#endif
		page_ids = reinterpret_cast<page_id_t *>(current_buffer->Ptr() + offset);
		offset += (sizeof(page_id_t) * total_vector_count);

		offset = AlignValue<idx_t, sizeof(page_offset_t)>(offset);
		page_offsets = reinterpret_cast<page_offset_t *>(current_buffer->Ptr() + offset);
		offset += (sizeof(page_offset_t) * total_vector_count);

		offset = AlignValue<idx_t, sizeof(uncompressed_size_t)>(offset);
		uncompressed_sizes = reinterpret_cast<uncompressed_size_t *>(current_buffer->Ptr() + offset);
		offset += (sizeof(uncompressed_size_t) * total_vector_count);

		offset = AlignValue<idx_t, sizeof(compressed_size_t)>(offset);
		compressed_sizes = reinterpret_cast<compressed_size_t *>(current_buffer->Ptr() + offset);
		offset += (sizeof(compressed_size_t) * total_vector_count);
		current_buffer_ptr = current_buffer->Ptr() + offset;

#ifdef DEBUG
		D_ASSERT(uint64_t(current_buffer_ptr - start_of_vector_metadata) == analyze_state->GetVectorMetadataSize());
#endif
		D_ASSERT(GetCurrentOffset() <= GetWritableSpace());
	}

public:
	void ResetOutBuffer() {
		out_buffer.dst = current_buffer_ptr;
		out_buffer.pos = 0;

		auto remaining_space = info.GetBlockSize() - GetCurrentOffset() - sizeof(block_id_t);
		out_buffer.size = remaining_space;
	}

	void SetCurrentBuffer(BufferHandle &handle) {
		current_buffer = &handle;
		current_buffer_ptr = handle.Ptr();
	}

	BufferHandle &GetExtraPageBuffer(block_id_t current_block_id) {
		auto &block_manager = partial_block_manager.GetBlockManager();
		auto &buffer_manager = block_manager.buffer_manager;

		optional_ptr<BufferHandle> to_use;

		if (in_vector) {
			// Currently in a Vector, we have to be mindful of the buffer that the string_lengths lives on
			// as that will have to stay writable until the Vector is finished
			bool already_separated = current_buffer != vector_lengths_buffer;
			if (already_separated) {
				// Already separated, can keep using the other buffer (flush it first)
				FlushPage(*current_buffer, current_block_id);
				to_use = current_buffer;
			} else {
				// Not already separated, have to use the other page
				to_use = current_buffer == &extra_pages[0] ? &extra_pages[1] : &extra_pages[0];
			}
		} else {
			// Start of a new Vector, the string_lengths did not fit on the previous page
			bool previous_page_is_segment = current_buffer == &segment_handle;
			if (!previous_page_is_segment) {
				// We're asking for a fresh buffer to start the vectors data
				// that means the previous vector is finished - so we can flush the current page and reuse it
				D_ASSERT(current_block_id != INVALID_BLOCK);
				FlushPage(*current_buffer, current_block_id);
				to_use = current_buffer;
			} else {
				// Previous buffer was the segment, take the first extra page in this case
				to_use = &extra_pages[0];
			}
		}

		if (!to_use->IsValid()) {
			*to_use = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, block_manager.GetBlockSize());
		}
		return *to_use;
	}

	void InitializeVector() {
		D_ASSERT(!in_vector);
		if (vector_count + 1 >= total_vector_count) {
			vector_size = analyze_state->count - (ZSTD_VECTOR_SIZE * vector_count);
		} else {
			vector_size = ZSTD_VECTOR_SIZE;
		}
		auto current_offset = GetCurrentOffset();
		current_offset = UnsafeNumericCast<page_offset_t>(
		    AlignValue<idx_t, sizeof(string_length_t)>(UnsafeNumericCast<idx_t>(current_offset)));
		current_buffer_ptr = current_buffer->Ptr() + current_offset;
		compressed_size = 0;
		uncompressed_size = 0;

		if (current_offset + (vector_size * sizeof(string_length_t)) >= GetWritableSpace()) {
			NewPage();
		}
		current_offset = GetCurrentOffset();
		starting_offset = current_offset;
		starting_page = GetCurrentId();

		vector_lengths_buffer = current_buffer;
		string_lengths = reinterpret_cast<string_length_t *>(current_buffer->Ptr() + current_offset);
		current_buffer_ptr = reinterpret_cast<data_ptr_t>(string_lengths);
		current_buffer_ptr += vector_size * sizeof(string_length_t);
		// 'out_buffer' should be set to point directly after the string_lengths
		ResetOutBuffer();

		// Initialize the context for streaming compression
		duckdb_zstd::ZSTD_CCtx_reset(analyze_state->context, duckdb_zstd::ZSTD_reset_session_only);
		duckdb_zstd::ZSTD_CCtx_refCDict(analyze_state->context, analyze_state->compression_dict);
		duckdb_zstd::ZSTD_CCtx_setParameter(analyze_state->context, duckdb_zstd::ZSTD_c_compressionLevel,
		                                    GetCompressionLevel());
		in_vector = true;
	}

	void CompressString(const string_t &string, bool end_of_vector) {
		duckdb_zstd::ZSTD_inBuffer in_buffer = {/*data = */ string.GetData(),
		                                        /*length = */ string.GetSize(),
		                                        /*pos = */ 0};

		if (!end_of_vector && string.GetSize() == 0) {
			return;
		}
		uncompressed_size += string.GetSize();
		const auto end_mode = end_of_vector ? duckdb_zstd::ZSTD_e_end : duckdb_zstd::ZSTD_e_continue;

		size_t compress_result;
		while (true) {
			idx_t old_pos = out_buffer.pos;

			compress_result =
			    duckdb_zstd::ZSTD_compressStream2(analyze_state->context, &out_buffer, &in_buffer, end_mode);
			D_ASSERT(out_buffer.pos >= old_pos);
			auto diff = out_buffer.pos - old_pos;
			compressed_size += diff;
			current_buffer_ptr += diff;

			if (duckdb_zstd::ZSTD_isError(compress_result)) {
				throw InvalidInputException("ZSTD Compression failed: %s",
				                            duckdb_zstd::ZSTD_getErrorName(compress_result));
			}
			if (compress_result == 0) {
				// Finished
				break;
			}
			if (out_buffer.pos != out_buffer.size) {
				throw InternalException("Expected ZSTD_compressStream2 to fully utilize the current buffer, but pos is "
				                        "%d, while size is %d",
				                        out_buffer.pos, out_buffer.size);
			}
			NewPage();
		}
	}

	void AddString(const string_t &string) {
		if (!tuple_count) {
			InitializeVector();
		}

		string_lengths[tuple_count] = UnsafeNumericCast<string_length_t>(string.GetSize());
		bool final_tuple = tuple_count + 1 >= vector_size;
		CompressString(string, final_tuple);

		tuple_count++;
		if (tuple_count == vector_size) {
			// Reached the end of this vector
			FlushVector();
		}

		UncompressedStringStorage::UpdateStringStats(segment->stats, string);
	}

	idx_t GetWritableSpace() {
		return info.GetBlockSize() - sizeof(block_id_t);
	}

	void NewPage(bool additional_data_page = false) {
		block_id_t new_id = FinalizePage();
		block_id_t current_block_id = block_id;
		auto &buffer = GetExtraPageBuffer(current_block_id);
		block_id = new_id;
		SetCurrentBuffer(buffer);
		ResetOutBuffer();
	}

	block_id_t FinalizePage() {
		auto &block_manager = partial_block_manager.GetBlockManager();
		auto new_id = block_manager.GetFreeBlockId();
		auto &state = segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		state.RegisterBlock(block_manager, new_id);

		D_ASSERT(GetCurrentOffset() <= GetWritableSpace());

		// Write the new id at the end of the last page
		Store<block_id_t>(new_id, current_buffer_ptr);
		current_buffer_ptr += sizeof(block_id_t);
		return new_id;
	}

	void FlushPage(BufferHandle &buffer, block_id_t block_id) {
		if (block_id == INVALID_BLOCK) {
			return;
		}

		// Write the current page to disk
		auto &block_manager = partial_block_manager.GetBlockManager();
		block_manager.Write(buffer.GetFileBuffer(), block_id);
		{
			auto lock = partial_block_manager.GetLock();
			partial_block_manager.AddWrittenBlock(block_id);
		}
	}

	void FlushVector() {
		// Write the metadata for this Vector
		page_ids[vector_count] = starting_page;
		page_offsets[vector_count] = starting_offset;
		compressed_sizes[vector_count] = compressed_size;
		uncompressed_sizes[vector_count] = uncompressed_size;
		vector_count++;
		in_vector = false;

		const bool is_last_vector = vector_count == total_vector_count;
		tuple_count = 0;
		if (is_last_vector) {
			FlushPage(*current_buffer, block_id);
			if (starting_page != block_id) {
				FlushPage(*vector_lengths_buffer, starting_page);
			}
		} else {
			if (vector_lengths_buffer == current_buffer) {
				// We did not cross a page boundary writing this vector
				return;
			}
			// Flush the page that holds the vector lengths
			FlushPage(*vector_lengths_buffer, starting_page);
		}
	}

	page_id_t GetCurrentId() {
		if (&segment_handle == current_buffer.get()) {
			return INVALID_BLOCK;
		}
		return block_id;
	}

	page_offset_t GetCurrentOffset() {
		auto &handle = *current_buffer;
		auto start_of_buffer = handle.Ptr();
		D_ASSERT(current_buffer_ptr >= start_of_buffer);
		auto res = (page_offset_t)(current_buffer_ptr - start_of_buffer);
		D_ASSERT(res <= GetWritableSpace());
		return res;
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

	void AddNull() {
		AddString("");
	}

public:
	unique_ptr<ZSTDAnalyzeState> analyze_state;
	ColumnDataCheckpointer &checkpointer;
	PartialBlockManager &partial_block_manager;
	CompressionFunction &function;

	// The segment state
	unique_ptr<ColumnSegment> segment;
	BufferHandle segment_handle;

	// Non-segment buffers
	BufferHandle extra_pages[2];
	block_id_t block_id = INVALID_BLOCK;

	// Current block state
	optional_ptr<BufferHandle> current_buffer;
	//! The buffer that contains the vector lengths
	optional_ptr<BufferHandle> vector_lengths_buffer;
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
	//! Whether we are currently in a Vector
	bool in_vector = false;
	//! The compression context indicating where we are in the output buffer
	duckdb_zstd::ZSTD_outBuffer out_buffer;
	idx_t uncompressed_size = 0;
	idx_t compressed_size = 0;
	string_length_t *string_lengths;

	//! Amount of tuples we have seen for the current vector
	idx_t tuple_count = 0;
	//! The expected size of this vector (ZSTD_VECTOR_SIZE except for the last one)
	idx_t vector_size;
};

// Iterate through the vectors of the segment again, performing more expensive calculations
// since this will only be run when the compression algorithm has already been chosen
void ZSTDStorage::PrepareCompress(AnalyzeState &analyze_state, Vector &input, idx_t count) {
	auto &state = analyze_state.Cast<ZSTDAnalyzeState>();
	auto &prepare = state.prepare_state;

	if (!prepare.sample_buffer) {
		prepare.sample_buffer = Allocator::DefaultAllocator().Allocate(prepare.dict_size * 100);
		prepare.combine_vectors = (state.count / ZSTD_VECTOR_SIZE) > ZSTD_MIN_SAMPLE_COUNT;
	}

	if (prepare.finished) {
		return;
	}

	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	idx_t sum = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		auto &str = data[idx];
		auto string_size = str.GetSize();
		sum += string_size;
		if (prepare.sample_buffer_offset + string_size >= prepare.sample_buffer.GetSize()) {
			prepare.finished = true;
			break;
		}
		memcpy(prepare.sample_buffer.get() + prepare.sample_buffer_offset, str.GetData(), string_size);
		if (!prepare.combine_vectors) {
			prepare.sample_sizes.push_back(string_size);
		}
		prepare.sample_buffer_offset += string_size;
	}

	if (prepare.combine_vectors) {
		prepare.sample_sizes.push_back(sum);
	}

	prepare.prepare_count += count;
	if (prepare.prepare_count >= state.count) {
		prepare.finished = true;
	}
	if (prepare.finished) {
		//! Train the dictionary on the collected samples
		prepare.dict = CreateDictFromSamples(prepare.dict_size, prepare.sample_sizes, prepare.sample_buffer);
		prepare.sample_sizes.clear();
		prepare.sample_buffer.Reset();
	}
}

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
	//! The index of the (internal) vector being read
	idx_t vector_idx;
	block_id_t block_id;
	page_offset_t block_offset;

	uncompressed_size_t uncompressed_size;
	compressed_size_t compressed_size;

	//! The amount of tuples that are in this Vector
	idx_t count;
};

struct ZSTDVectorScanState {
public:
	ZSTDVectorScanState() {
	}
	ZSTDVectorScanState(ZSTDVectorScanState &&other) = default;
	ZSTDVectorScanState(const ZSTDVectorScanState &other) = delete;

public:
	//! The metadata of the vector
	ZSTDVectorScanMetadata metadata;
	//! The (pinned) buffer handle(s) for this vectors data
	vector<BufferHandle> buffer_handles;
	//! The current pointer at which we're reading the vectors data
	data_ptr_t current_buffer_ptr;
	//! The (uncompressed) string lengths for this vector
	string_length_t *string_lengths;
	//! The amount of values already consumed from the state
	idx_t scanned_count = 0;
	//! The amount of compressed data read
	idx_t compressed_scan_count = 0;
	//! The inBuffer that ZSTD_decompressStream reads the compressed data from
	duckdb_zstd::ZSTD_inBuffer in_buffer;
};

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct ZSTDScanState : public SegmentScanState {
public:
	explicit ZSTDScanState(ColumnSegment &segment)
	    : state(segment.GetSegmentState()->Cast<UncompressedStringSegmentState>()),
	      block_manager(segment.GetBlockManager()), buffer_manager(BufferManager::GetBufferManager(segment.db)),
	      decompression_dict(nullptr), segment_block_offset(segment.GetBlockOffset()) {
		decompression_context = duckdb_zstd::ZSTD_createDCtx();
		segment_handle = buffer_manager.Pin(segment.block);

		// Load dictionary
		auto data = segment_handle.Ptr() + segment.GetBlockOffset();
		auto dict_size = Load<dict_size_t>(data);

		dict = DictBuffer(data + sizeof(dict_size_t), dict_size);

		idx_t offset = 0;
		decompression_dict = duckdb_zstd::ZSTD_createDDict_byReference(dict.Buffer(), dict.Size());
		offset += sizeof(dict_size_t) + dict.Size();

		segment_count = segment.count.load();
		idx_t amount_of_vectors = (segment_count / ZSTD_VECTOR_SIZE) + ((segment_count % ZSTD_VECTOR_SIZE) != 0);

		// Set pointers to the Vector Metadata
		offset = AlignValue<idx_t, sizeof(page_id_t)>(offset);
		page_ids = reinterpret_cast<page_id_t *>(data + offset);
		offset += (sizeof(page_id_t) * amount_of_vectors);

		offset = AlignValue<idx_t, sizeof(page_offset_t)>(offset);
		page_offsets = reinterpret_cast<page_offset_t *>(data + offset);
		offset += (sizeof(page_offset_t) * amount_of_vectors);

		offset = AlignValue<idx_t, sizeof(uncompressed_size_t)>(offset);
		uncompressed_sizes = reinterpret_cast<uncompressed_size_t *>(data + offset);
		offset += (sizeof(uncompressed_size_t) * amount_of_vectors);

		offset = AlignValue<idx_t, sizeof(compressed_size_t)>(offset);
		compressed_sizes = reinterpret_cast<compressed_size_t *>(data + offset);
		offset += (sizeof(compressed_size_t) * amount_of_vectors);

		scanned_count = 0;
	}
	~ZSTDScanState() override {
		duckdb_zstd::ZSTD_freeDDict(decompression_dict);
		duckdb_zstd::ZSTD_freeDCtx(decompression_context);
	}

public:
	idx_t GetVectorIndex(idx_t start_index, idx_t &offset) {
		idx_t vector_idx = start_index / ZSTD_VECTOR_SIZE;
		offset = start_index % ZSTD_VECTOR_SIZE;
		return vector_idx;
	}

	ZSTDVectorScanMetadata GetVectorMetadata(idx_t vector_idx) {
		idx_t previous_value_count = vector_idx * ZSTD_VECTOR_SIZE;
		idx_t value_count = MinValue<idx_t>(segment_count - previous_value_count, ZSTD_VECTOR_SIZE);

		return ZSTDVectorScanMetadata {/* vector_idx = */ vector_idx,
		                               /* block_id = */ page_ids[vector_idx],
		                               /* block_offset = */ page_offsets[vector_idx],
		                               /* uncompressed_size = */ uncompressed_sizes[vector_idx],
		                               /* compressed_size = */ compressed_sizes[vector_idx],
		                               /* count = */ value_count};
	}

	shared_ptr<BlockHandle> LoadPage(block_id_t block_id) {
		return state.GetHandle(block_manager, block_id);
	}

	bool UseVectorStateCache(idx_t vector_idx, idx_t internal_offset) {
		if (!current_vector) {
			// No vector loaded yet
			return false;
		}
		if (current_vector->metadata.vector_idx != vector_idx) {
			// Not the same vector
			return false;
		}
		if (current_vector->scanned_count != internal_offset) {
			// Not the same scan offset
			return false;
		}
		return true;
	}

	ZSTDVectorScanState &LoadVector(idx_t vector_idx, idx_t internal_offset) {
		if (UseVectorStateCache(vector_idx, internal_offset)) {
			return *current_vector;
		}
		current_vector = make_uniq<ZSTDVectorScanState>();
		current_vector->metadata = GetVectorMetadata(vector_idx);
		auto &metadata = current_vector->metadata;
		auto &scan_state = *current_vector;
		data_ptr_t handle_start;
		idx_t ptr_offset = 0;
		if (metadata.block_id == INVALID_BLOCK) {
			// Data lives on the segment's page
			handle_start = segment_handle.Ptr();
			ptr_offset += segment_block_offset;
		} else {
			// Data lives on an extra page, have to load the block first
			auto block = LoadPage(metadata.block_id);
			auto data_handle = buffer_manager.Pin(block);
			handle_start = data_handle.Ptr();
			scan_state.buffer_handles.push_back(std::move(data_handle));
		}

		ptr_offset += metadata.block_offset;
		ptr_offset = AlignValue<idx_t, sizeof(string_length_t)>(ptr_offset);
		scan_state.current_buffer_ptr = handle_start + ptr_offset;

		auto vector_size = metadata.count;

		scan_state.string_lengths = reinterpret_cast<string_length_t *>(scan_state.current_buffer_ptr);
		scan_state.current_buffer_ptr += (sizeof(string_length_t) * vector_size);

		// Update the in_buffer to point to the start of the compressed data frame
		idx_t current_offset = UnsafeNumericCast<idx_t>(scan_state.current_buffer_ptr - handle_start);
		scan_state.in_buffer.src = scan_state.current_buffer_ptr;
		scan_state.in_buffer.pos = 0;
		scan_state.in_buffer.size = block_manager.GetBlockSize() - sizeof(block_id_t) - current_offset;

		// Initialize the context for streaming decompression
		duckdb_zstd::ZSTD_DCtx_reset(decompression_context, duckdb_zstd::ZSTD_reset_session_only);
		duckdb_zstd::ZSTD_DCtx_refDDict(decompression_context, decompression_dict);

		if (internal_offset) {
			Skip(scan_state, internal_offset);
		}
		return scan_state;
	}

	void LoadNextPageForVector(ZSTDVectorScanState &scan_state) {
		if (scan_state.in_buffer.pos != scan_state.in_buffer.size) {
			throw InternalException(
			    "(ZSTDScanState::LoadNextPageForVector) Trying to load the next page before consuming the current one");
		}
		// Read the next block id from the end of the page
		auto base_ptr =
		    reinterpret_cast<data_ptr_t>(const_cast<void *>(scan_state.in_buffer.src)); // NOLINT: const cast
		auto next_id_ptr = base_ptr + scan_state.in_buffer.size;
		block_id_t next_id = Load<block_id_t>(next_id_ptr);

		// Load the next page
		auto block = LoadPage(next_id);
		auto handle = buffer_manager.Pin(block);
		auto ptr = handle.Ptr();
		scan_state.buffer_handles.push_back(std::move(handle));
		scan_state.current_buffer_ptr = ptr;

		// Update the in_buffer to point to the new page
		scan_state.in_buffer.src = ptr;
		scan_state.in_buffer.pos = 0;

		idx_t page_size = block_manager.GetBlockSize() - sizeof(block_id_t);
		idx_t remaining_compressed_data = scan_state.metadata.compressed_size - scan_state.compressed_scan_count;
		scan_state.in_buffer.size = MinValue<idx_t>(page_size, remaining_compressed_data);
	}

	void DecompressString(ZSTDVectorScanState &scan_state, data_ptr_t destination, idx_t uncompressed_length) {
		if (uncompressed_length == 0) {
			return;
		}

		duckdb_zstd::ZSTD_outBuffer out_buffer;

		out_buffer.dst = destination;
		out_buffer.pos = 0;
		out_buffer.size = uncompressed_length;

		while (true) {
			idx_t old_pos = scan_state.in_buffer.pos;
			size_t res = duckdb_zstd::ZSTD_decompressStream(
			    /* zds = */ decompression_context,
			    /* output =*/&out_buffer,
			    /* input =*/&scan_state.in_buffer);
			scan_state.compressed_scan_count += scan_state.in_buffer.pos - old_pos;
			if (duckdb_zstd::ZSTD_isError(res)) {
				throw InvalidInputException("ZSTD Decompression failed: %s", duckdb_zstd::ZSTD_getErrorName(res));
			}
			if (out_buffer.pos == out_buffer.size) {
				break;
			}
			// Did not fully decompress, it needs a new page to read from
			LoadNextPageForVector(scan_state);
		}
	}

	void Skip(ZSTDVectorScanState &scan_state, idx_t count) {
		if (!skip_buffer) {
			skip_buffer = Allocator::DefaultAllocator().Allocate(duckdb_zstd::ZSTD_DStreamOutSize());
		}

		D_ASSERT(scan_state.scanned_count + count <= scan_state.metadata.count);

		// Figure out how much we need to skip
		string_length_t *string_lengths = &scan_state.string_lengths[scan_state.scanned_count];
		idx_t uncompressed_length = 0;
		for (idx_t i = 0; i < count; i++) {
			uncompressed_length += string_lengths[i];
		}

		// Skip that many bytes by decompressing into the skip_buffer
		idx_t remaining = uncompressed_length;
		while (remaining) {
			idx_t to_scan = MinValue<idx_t>(skip_buffer.GetSize(), remaining);
			DecompressString(scan_state, skip_buffer.get(), to_scan);
			remaining -= to_scan;
		}
		scan_state.scanned_count += count;
		scanned_count += count;
	}

	void ScanInternal(ZSTDVectorScanState &scan_state, idx_t count, Vector &result, idx_t result_offset) {
		D_ASSERT(scan_state.scanned_count + count <= scan_state.metadata.count);
		D_ASSERT(result.GetType().id() == LogicalTypeId::VARCHAR);

		string_length_t *string_lengths = &scan_state.string_lengths[scan_state.scanned_count];
		idx_t uncompressed_length = 0;
		for (idx_t i = 0; i < count; i++) {
			uncompressed_length += string_lengths[i];
		}
		auto empty_string = StringVector::EmptyString(result, uncompressed_length);
		auto uncompressed_data = empty_string.GetDataWriteable();
		auto string_data = FlatVector::GetData<string_t>(result);

		DecompressString(scan_state, reinterpret_cast<data_ptr_t>(uncompressed_data), uncompressed_length);

		idx_t offset = 0;
		auto uncompressed_data_const = empty_string.GetData();
		for (idx_t i = 0; i < count; i++) {
			string_data[result_offset + i] = string_t(uncompressed_data_const + offset, string_lengths[i]);
			offset += string_lengths[i];
		}
		scan_state.scanned_count += count;
		scanned_count += count;
	}

	void ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count) {
		D_ASSERT(offset + count <= ZSTD_VECTOR_SIZE);

		idx_t remaining = count;
		idx_t scanned = 0;
		while (remaining) {
			idx_t internal_offset;
			idx_t vector_idx = GetVectorIndex(start_idx + scanned, internal_offset);
			auto &scan_state = LoadVector(vector_idx, internal_offset);
			idx_t remaining_in_vector = scan_state.metadata.count - scan_state.scanned_count;
			idx_t to_scan = MinValue<idx_t>(remaining, remaining_in_vector);
			ScanInternal(scan_state, to_scan, result, offset + scanned);
			remaining -= to_scan;
			scanned += to_scan;
		}
		D_ASSERT(scanned == count);
	}

public:
	UncompressedStringSegmentState &state;
	BlockManager &block_manager;
	BufferManager &buffer_manager;

	DictBuffer dict;
	duckdb_zstd::ZSTD_DCtx *decompression_context = nullptr;
	duckdb_zstd::ZSTD_DDict *decompression_dict = nullptr;

	idx_t segment_block_offset;
	BufferHandle segment_handle;

	//===--------------------------------------------------------------------===//
	// Vector metadata
	//===--------------------------------------------------------------------===//
	page_id_t *page_ids;
	page_offset_t *page_offsets;
	uncompressed_size_t *uncompressed_sizes;
	compressed_size_t *compressed_sizes;

	//! Cache of (the scan state of) the current vector being read
	unique_ptr<ZSTDVectorScanState> current_vector;

	//! The amount of tuples stored in the segment
	idx_t segment_count;
	//! The amount of tuples consumed
	idx_t scanned_count = 0;

	//! Buffer for skipping data
	AllocatedData skip_buffer;
};

unique_ptr<SegmentScanState> ZSTDStorage::StringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<ZSTDScanState>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	auto &scan_state = state.scan_state->template Cast<ZSTDScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	scan_state.ScanPartial(start, result, result_offset, scan_count);
}

void ZSTDStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {
	ZSTDScanState scan_state(segment);
	scan_state.ScanPartial(UnsafeNumericCast<idx_t>(row_id), result, result_idx, 1);
}

//===--------------------------------------------------------------------===//
// Serialization & Cleanup
//===--------------------------------------------------------------------===//

unique_ptr<CompressedSegmentState> ZSTDStorage::StringInitSegment(ColumnSegment &segment, block_id_t block_id,
                                                                  optional_ptr<ColumnSegmentState> segment_state) {
	auto result = make_uniq<UncompressedStringSegmentState>();
	if (segment_state) {
		auto &serialized_state = segment_state->Cast<SerializedStringSegmentState>();
		result->on_disk_blocks = std::move(serialized_state.blocks);
	}
	return std::move(result);
}

unique_ptr<ColumnSegmentState> ZSTDStorage::SerializeState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.on_disk_blocks.empty()) {
		// no on-disk blocks - nothing to write
		return nullptr;
	}
	return make_uniq<SerializedStringSegmentState>(state.on_disk_blocks);
}

unique_ptr<ColumnSegmentState> ZSTDStorage::DeserializeState(Deserializer &deserializer) {
	auto result = make_uniq<SerializedStringSegmentState>();
	deserializer.ReadProperty(1, "overflow_blocks", result->blocks);
	return std::move(result);
}

void ZSTDStorage::CleanupState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	auto &block_manager = segment.GetBlockManager();
	for (auto &block_id : state.on_disk_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ZSTDFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	auto zstd = CompressionFunction(
	    CompressionType::COMPRESSION_ZSTD, data_type, ZSTDStorage::StringInitAnalyze, ZSTDStorage::StringAnalyze,
	    ZSTDStorage::StringFinalAnalyze, ZSTDStorage::PrepareCompress, ZSTDStorage::InitCompression,
	    ZSTDStorage::Compress, ZSTDStorage::FinalizeCompress, ZSTDStorage::StringInitScan, ZSTDStorage::StringScan,
	    ZSTDStorage::StringScanPartial, ZSTDStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
	zstd.init_segment = ZSTDStorage::StringInitSegment;
	zstd.serialize_state = ZSTDStorage::SerializeState;
	zstd.deserialize_state = ZSTDStorage::DeserializeState;
	zstd.cleanup_state = ZSTDStorage::CleanupState;
	return zstd;
}

bool ZSTDFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

} // namespace duckdb
