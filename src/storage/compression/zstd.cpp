#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"

#include "zstd.h"

/*
Data layout per segment:
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

using page_id_t = int64_t;
using page_offset_t = uint32_t;
using uncompressed_size_t = uint64_t;
using compressed_size_t = uint64_t;
using string_length_t = uint32_t;

static int32_t GetCompressionLevel() {
	return duckdb_zstd::ZSTD_defaultCLevel();
}

static constexpr idx_t ZSTD_VECTOR_SIZE = STANDARD_VECTOR_SIZE > 2048 ? STANDARD_VECTOR_SIZE : 2048;

namespace duckdb {

static idx_t GetWritableSpace(const CompressionInfo &info) {
	return info.GetBlockSize() - sizeof(block_id_t);
}

static idx_t GetVectorCount(idx_t count) {
	idx_t vector_count = count / ZSTD_VECTOR_SIZE;
	vector_count += (count % ZSTD_VECTOR_SIZE) != 0;
	return vector_count;
}

static idx_t GetVectorMetadataSize(idx_t vector_count) {
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

struct ZSTDStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(const QueryContext &context, ColumnSegment &segment);
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
	static void StringSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
		// NO OP
	}

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

struct ZSTDAnalyzeState : public AnalyzeState {
public:
	ZSTDAnalyzeState(CompressionInfo &info, DBConfig &config) : AnalyzeState(info), config(config), context(nullptr) {
		context = duckdb_zstd::ZSTD_createCCtx();
	}
	~ZSTDAnalyzeState() override {
		duckdb_zstd::ZSTD_freeCCtx(context);
	}

public:
	inline void AppendString(const string_t &str) {
		auto string_size = str.GetSize();
		total_size += string_size;
	}

public:
	DBConfig &config;

	duckdb_zstd::ZSTD_CCtx *context;
	//! The combined string lengths for all values in the segment
	idx_t total_size = 0;
	//! The total amount of values in the segment
	idx_t count = 0;

	//! The amount of vectors per filled segment
	idx_t vectors_per_segment = 0;
	//! The total amount of segments we will create
	idx_t segment_count = 0;
	//! Current vector in the segment
	idx_t vectors_in_segment = 0;
	//! Current amount of values in the vector
	idx_t values_in_vector = 0;
};

unique_ptr<AnalyzeState> ZSTDStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// check if the storage version we are writing to supports sztd
	auto &storage = col_data.GetStorageManager();
	auto &block_manager = col_data.GetBlockManager();
	if (block_manager.InMemory()) {
		//! Can't use ZSTD in in-memory environment
		return nullptr;
	}
	if (storage.GetStorageVersion() < 4) {
		// compatibility mode with old versions - disable zstd
		return nullptr;
	}
	CompressionInfo info(col_data.GetBlockManager());
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
	state.values_in_vector += count;
	while (state.values_in_vector >= ZSTD_VECTOR_SIZE) {
		if (GetVectorMetadataSize(state.vectors_in_segment + 1) > GetWritableSpace(state.info)) {
			state.vectors_per_segment = state.vectors_in_segment;
			state.segment_count++;
			state.vectors_in_segment = 0;
		} else {
			state.vectors_in_segment++;
		}
		state.values_in_vector -= ZSTD_VECTOR_SIZE;
	}
	state.count += count;
	return true;
}

// Compression score to determine which compression to use
idx_t ZSTDStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();

	if (!state.count) {
		return NumericLimits<idx_t>::Maximum();
	}

	if (state.values_in_vector) {
		D_ASSERT(state.values_in_vector < ZSTD_VECTOR_SIZE);
		state.segment_count++;
	}

	double penalty;
	idx_t average_length = state.total_size / state.count;
	auto threshold = state.config.options.zstd_min_string_length;
	if (average_length >= threshold) {
		penalty = 1.0;
	} else {
		// set a high penalty if we are below the threshold - this still allows ZSTD to be forced
		// but makes it very unlikely to be chosen automatically
		penalty = 1000.0;
	}

	auto expected_compressed_size = (double)state.total_size / 2.0;

	idx_t estimated_size = 0;
	estimated_size += LossyNumericCast<idx_t>(expected_compressed_size);

	estimated_size += state.count * sizeof(string_length_t);
	estimated_size += GetVectorMetadataSize(GetVectorCount(state.count));

	return LossyNumericCast<idx_t>((double)estimated_size * penalty);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

class ZSTDCompressionState : public CompressionState {
public:
	explicit ZSTDCompressionState(ColumnDataCheckpointData &checkpoint_data,
	                              unique_ptr<ZSTDAnalyzeState> &&analyze_state_p)
	    : CompressionState(analyze_state_p->info), analyze_state(std::move(analyze_state_p)),
	      checkpoint_data(checkpoint_data),
	      partial_block_manager(checkpoint_data.GetCheckpointState().GetPartialBlockManager()),
	      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_ZSTD)) {
		total_vector_count = GetVectorCount(analyze_state->count);
		total_segment_count = analyze_state->segment_count;
		vectors_per_segment = analyze_state->vectors_per_segment;

		segment_count = 0;
		vector_count = 0;
		vector_in_segment_count = 0;
		tuple_count = 0;

		idx_t offset = NewSegment();
		SetCurrentBuffer(segment_handle);
		current_buffer_ptr = segment_handle.Ptr() + offset;
		D_ASSERT(GetCurrentOffset() <= GetWritableSpace(info));
	}

public:
	void ResetOutBuffer() {
		D_ASSERT(GetCurrentOffset() <= GetWritableSpace(info));
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
			*to_use = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, &block_manager);
		}
		return *to_use;
	}

	idx_t NewSegment() {
		if (current_buffer == &segment_handle) {
			// This should never happen, the string lengths + vector metadata size should always exceed a page size,
			// even if the strings are all empty
			throw InternalException("We are asking for a new segment, but somehow we're still writing vector data onto "
			                        "the initial (segment) page");
		}
		FlushSegment();
		CreateEmptySegment();

		// Figure out how many vectors we are storing in this segment
		idx_t vectors_in_segment;
		if (segment_count + 1 >= total_segment_count) {
			vectors_in_segment = total_vector_count - vector_count;
		} else {
			vectors_in_segment = vectors_per_segment;
		}

		idx_t offset = 0;
		page_ids = reinterpret_cast<page_id_t *>(segment_handle.Ptr() + offset);
		offset += (sizeof(page_id_t) * vectors_in_segment);

		offset = AlignValue<idx_t, sizeof(page_offset_t)>(offset);
		page_offsets = reinterpret_cast<page_offset_t *>(segment_handle.Ptr() + offset);
		offset += (sizeof(page_offset_t) * vectors_in_segment);

		offset = AlignValue<idx_t, sizeof(uncompressed_size_t)>(offset);
		uncompressed_sizes = reinterpret_cast<uncompressed_size_t *>(segment_handle.Ptr() + offset);
		offset += (sizeof(uncompressed_size_t) * vectors_in_segment);

		offset = AlignValue<idx_t, sizeof(compressed_size_t)>(offset);
		compressed_sizes = reinterpret_cast<compressed_size_t *>(segment_handle.Ptr() + offset);
		offset += (sizeof(compressed_size_t) * vectors_in_segment);

		D_ASSERT(offset == GetVectorMetadataSize(vectors_in_segment));
		return offset;
	}

	void InitializeVector() {
		D_ASSERT(!in_vector);
		if (vector_count + 1 >= total_vector_count) {
			//! Last vector
			vector_size = analyze_state->count - (ZSTD_VECTOR_SIZE * vector_count);
		} else {
			vector_size = ZSTD_VECTOR_SIZE;
		}
		auto current_offset = GetCurrentOffset();
		current_offset = UnsafeNumericCast<page_offset_t>(
		    AlignValue<idx_t, sizeof(string_length_t)>(UnsafeNumericCast<idx_t>(current_offset)));
		current_buffer_ptr = current_buffer->Ptr() + current_offset;
		D_ASSERT(GetCurrentOffset() <= GetWritableSpace(info));
		compressed_size = 0;
		uncompressed_size = 0;

		if (GetVectorMetadataSize(vector_in_segment_count + 1) > GetWritableSpace(info)) {
			D_ASSERT(vector_in_segment_count <= vectors_per_segment);
			// Can't fit this vector on this segment anymore, have to flush and a grab new one
			NewSegment();
		}

		if (current_offset + (vector_size * sizeof(string_length_t)) >= GetWritableSpace(info)) {
			// Check if there is room on the current page for the vector data
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
		duckdb_zstd::ZSTD_CCtx_refCDict(analyze_state->context, nullptr);
		duckdb_zstd::ZSTD_CCtx_setParameter(analyze_state->context, duckdb_zstd::ZSTD_c_compressionLevel,
		                                    GetCompressionLevel());
		in_vector = true;
	}

	void CompressString(const string_t &string, bool end_of_vector) {
		duckdb_zstd::ZSTD_inBuffer in_buffer = {/*data = */ string.GetData(),
		                                        /*length = */ size_t(string.GetSize()),
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
			D_ASSERT(GetCurrentOffset() <= GetWritableSpace(info));
			if (compress_result == 0) {
				// Finished
				break;
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

		D_ASSERT(GetCurrentOffset() <= GetWritableSpace(info));

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
		block_manager.Write(QueryContext(), buffer.GetFileBuffer(), block_id);
	}

	void FlushVector() {
		// Write the metadata for this Vector
		page_ids[vector_in_segment_count] = starting_page;
		page_offsets[vector_in_segment_count] = starting_offset;
		compressed_sizes[vector_in_segment_count] = compressed_size;
		uncompressed_sizes[vector_in_segment_count] = uncompressed_size;
		vector_count++;
		vector_in_segment_count++;
		in_vector = false;
		segment->count += tuple_count;

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
		D_ASSERT(res <= GetWritableSpace(info));
		return res;
	}

	void CreateEmptySegment() {
		auto &db = checkpoint_data.GetDatabase();
		auto &type = checkpoint_data.GetType();
		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, function, type, info.GetBlockSize(), info.GetBlockManager());
		segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
		segment_handle = buffer_manager.Pin(segment->block);
	}

	void FlushSegment() {
		if (!segment) {
			return;
		}
		auto &state = checkpoint_data.GetCheckpointState();
		idx_t segment_block_size;

		if (current_buffer.get() == &segment_handle) {
			segment_block_size = GetCurrentOffset();
		} else {
			// Block is fully used
			segment_block_size = info.GetBlockSize();
		}

		state.FlushSegment(std::move(segment), std::move(segment_handle), segment_block_size);
		segment_count++;
		vector_in_segment_count = 0;
	}

	void Finalize() {
		D_ASSERT(!tuple_count);
		FlushSegment();
		segment.reset();
	}

	void AddNull() {
		AddString("");
	}

public:
	unique_ptr<ZSTDAnalyzeState> analyze_state;
	ColumnDataCheckpointData &checkpoint_data;
	PartialBlockManager &partial_block_manager;
	CompressionFunction &function;

	// The segment state
	//! Current segment index we're at
	idx_t segment_count = 0;
	//! The total amount of segments we're writing
	idx_t total_segment_count = 0;
	//! The vectors to store in the last segment
	idx_t vectors_in_last_segment = 0;
	//! The vectors to store in a segment (not the last one)
	idx_t vectors_per_segment = 0;
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
	//! The amount of vectors we've seen in the current segment
	idx_t vector_in_segment_count = 0;
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

unique_ptr<CompressionState> ZSTDStorage::InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                          unique_ptr<AnalyzeState> analyze_state_p) {
	return make_uniq<ZSTDCompressionState>(checkpoint_data,
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
	      segment_block_offset(segment.GetBlockOffset()), segment(segment) {
		decompression_context = duckdb_zstd::ZSTD_createDCtx();
		segment_handle = buffer_manager.Pin(segment.block);

		auto data = segment_handle.Ptr() + segment.GetBlockOffset();
		idx_t offset = 0;

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

		auto string_lengths_size = (sizeof(string_length_t) * vector_size);
		scan_state.string_lengths = reinterpret_cast<string_length_t *>(scan_state.current_buffer_ptr);
		scan_state.current_buffer_ptr += string_lengths_size;

		// Update the in_buffer to point to the start of the compressed data frame
		idx_t current_offset = UnsafeNumericCast<idx_t>(scan_state.current_buffer_ptr - handle_start);
		scan_state.in_buffer.src = scan_state.current_buffer_ptr;
		scan_state.in_buffer.pos = 0;
		if (scan_state.metadata.block_offset + string_lengths_size + scan_state.metadata.compressed_size >
		    (segment.SegmentSize() - sizeof(block_id_t))) {
			//! We know that the compressed size is too big to fit on the current page
			scan_state.in_buffer.size =
			    MinValue(metadata.compressed_size, block_manager.GetBlockSize() - sizeof(block_id_t) - current_offset);
		} else {
			scan_state.in_buffer.size =
			    MinValue(metadata.compressed_size, block_manager.GetBlockSize() - current_offset);
		}

		// Initialize the context for streaming decompression
		duckdb_zstd::ZSTD_DCtx_reset(decompression_context, duckdb_zstd::ZSTD_reset_session_only);
		duckdb_zstd::ZSTD_DCtx_refDDict(decompression_context, nullptr);

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

		idx_t page_size = segment.SegmentSize() - sizeof(block_id_t);
		idx_t remaining_compressed_data = scan_state.metadata.compressed_size - scan_state.compressed_scan_count;
		scan_state.in_buffer.size = MinValue<idx_t>(page_size, remaining_compressed_data);
	}

	void DecompressString(ZSTDVectorScanState &scan_state, data_ptr_t destination, idx_t uncompressed_length) {
		if (uncompressed_length == 0) {
			return;
		}

		auto &in_buffer = scan_state.in_buffer;
		duckdb_zstd::ZSTD_outBuffer out_buffer;

		out_buffer.dst = destination;
		out_buffer.pos = 0;
		out_buffer.size = uncompressed_length;

		while (true) {
			idx_t old_pos = in_buffer.pos;
			size_t res = duckdb_zstd::ZSTD_decompressStream(
			    /* zds = */ decompression_context,
			    /* output =*/&out_buffer,
			    /* input =*/&in_buffer);
			scan_state.compressed_scan_count += in_buffer.pos - old_pos;
			if (duckdb_zstd::ZSTD_isError(res)) {
				throw InvalidInputException("ZSTD Decompression failed: %s", duckdb_zstd::ZSTD_getErrorName(res));
			}
			if (out_buffer.pos == out_buffer.size) {
				//! Done decompressing the relevant portion
				break;
			}
			if (!res) {
				D_ASSERT(out_buffer.pos == out_buffer.size);
				D_ASSERT(in_buffer.pos == in_buffer.size);
				break;
			}
			D_ASSERT(in_buffer.pos == in_buffer.size);
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
		D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);

		string_length_t *string_lengths = &scan_state.string_lengths[scan_state.scanned_count];
		idx_t uncompressed_length = 0;
		for (idx_t i = 0; i < count; i++) {
			uncompressed_length += string_lengths[i];
		}
		auto &buffer = StringVector::GetStringBuffer(result);
		auto uncompressed_data = buffer.AllocateShrinkableBuffer(uncompressed_length);
		auto string_data = FlatVector::GetData<string_t>(result);

		DecompressString(scan_state, uncompressed_data, uncompressed_length);

		idx_t offset = 0;
		for (idx_t i = 0; i < count; i++) {
			string_data[result_offset + i] = string_t(char_ptr_cast(uncompressed_data + offset), string_lengths[i]);
			offset += string_lengths[i];
		}
		scan_state.scanned_count += count;
		scanned_count += count;
	}

	void ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count) {
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

	duckdb_zstd::ZSTD_DCtx *decompression_context = nullptr;

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
	ColumnSegment &segment;

	//! Buffer for skipping data
	AllocatedData skip_buffer;
};

unique_ptr<SegmentScanState> ZSTDStorage::StringInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<ZSTDScanState>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	auto &scan_state = state.scan_state->template Cast<ZSTDScanState>();
	auto start = state.GetPositionInSegment();

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
	    ZSTDStorage::StringFinalAnalyze, ZSTDStorage::InitCompression, ZSTDStorage::Compress,
	    ZSTDStorage::FinalizeCompress, ZSTDStorage::StringInitScan, ZSTDStorage::StringScan,
	    ZSTDStorage::StringScanPartial, ZSTDStorage::StringFetchRow, ZSTDStorage::StringSkip);
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
