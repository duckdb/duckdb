#include "duckdb/common/constants.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/checkpoint/string_checkpoint_state.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/data_table_info.hpp"

#include "duckdb/storage/compression/zstd/zstd.hpp"
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
	static void VisitBlockIds(const ColumnSegment &segment, BlockIdVisitor &visitor);
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

	duckdb_zstd::ZSTD_CCtx *context = nullptr;
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
		state.vectors_in_segment++;
	}
	if (state.vectors_in_segment) {
		state.segment_count++;
	}

	double penalty;
	idx_t average_length = state.total_size / state.count;
	auto threshold = Settings::Get<ZstdMinStringLengthSetting>(state.config);
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
	      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_ZSTD)),
	      total_tuple_count(analyze_state->count), total_vector_count(GetVectorCount(total_tuple_count)),
	      total_segment_count(analyze_state->segment_count), vectors_per_segment(analyze_state->vectors_per_segment) {
		segment_count = 0;
		vector_count = 0;
		vector_state.tuple_count = 0;

		NewSegment();
		if (!(buffer_collection.GetCurrentOffset() <= GetWritableSpace(info))) {
			throw InternalException(
			    "(ZSTDCompressionState::ZSTDCompressionState) Offset (%d) exceeds writable space! (%d)",
			    buffer_collection.GetCurrentOffset(), GetWritableSpace(info));
		}
	}

public:
	void ResetOutBuffer() {
		if (!(buffer_collection.GetCurrentOffset() <= GetWritableSpace(info))) {
			throw InternalException("(ZSTDCompressionState::ResetOutBuffer) Offset (%d) exceeds writable space! (%d)",
			                        buffer_collection.GetCurrentOffset(), GetWritableSpace(info));
		}
		out_buffer.dst = buffer_collection.GetCurrentBufferPtr();
		out_buffer.pos = 0;

		auto remaining_space = info.GetBlockSize() - buffer_collection.GetCurrentOffset() - sizeof(block_id_t);
		out_buffer.size = remaining_space;
	}

	void WriteBlockIdPointer(page_id_t block_id) {
		auto ptr = buffer_collection.GetCurrentBufferPtr();
		Store<block_id_t>(block_id, ptr);
		buffer_collection.GetCurrentOffset() += sizeof(block_id_t);
	}

	void GetExtraPageBuffer(block_id_t current_block_id) {
		auto &block_manager = partial_block_manager.GetBlockManager();
		auto &buffer_manager = block_manager.buffer_manager;

		auto &current_buffer_state = buffer_collection.GetCurrentBufferState();
		current_buffer_state.full = true;

		if (buffer_collection.CanFlush()) {
			auto &buffer_state = buffer_collection.GetCurrentBufferState();
			FlushPage(buffer_collection.BufferHandleMutable(), current_block_id);
			buffer_state.flags.Clear();
			buffer_state.full = false;
			buffer_state.offset = 0;
			return;
		}

		//! Cycle through the extra pages, to figure out which one we can use
		//! In the worst case, the segment handle is entirely filled with vector metadata
		//! The last part of the first extra page is entirely filled with string metadata
		//! So we can only use the second extra page for data
		auto buffer_data = buffer_collection.GetBufferData(/*include_segment = */ false);
		for (auto &buffer : buffer_data) {
			auto &buffer_state = buffer.state;
			auto &flags = buffer_state.flags;
			if (flags.HasStringMetadata() || buffer_state.full) {
				continue;
			}
			buffer_collection.SetCurrentBuffer(buffer.slot);
			auto &buffer_handle = buffer_collection.BufferHandleMutable();
			if (!buffer_handle.IsValid()) {
				buffer_handle = buffer_manager.Allocate(MemoryTag::OVERFLOW_STRINGS, &block_manager);
			}
			return;
		}
		throw InternalException(
		    "(ZSTDCompressionState::GetExtraPageBuffer) Wasn't able to find a buffer to write overflow data to!");
	}

	void NewSegment() {
		if (buffer_collection.IsOnSegmentBuffer()) {
			// This should never happen, the string lengths + vector metadata size should always exceed a page size,
			// even if the strings are all empty
			throw InternalException("(ZSTDCompressionState::NewSegment) We are asking for a new segment, but somehow "
			                        "we're still writing vector data onto "
			                        "the initial (segment) page");
		}
		FlushSegment();
		CreateEmptySegment();

		// Figure out how many vectors we are storing in this segment
		idx_t vectors_in_segment;
		if (segment_count + 1 >= total_segment_count) {
			vectors_in_segment = total_vector_count - (segment_count * vectors_per_segment);
		} else {
			vectors_in_segment = vectors_per_segment;
		}

		buffer_collection.SetCurrentBuffer(ZSTDCompressionBufferCollection::Slot::SEGMENT);
		buffer_collection.buffer_states[0].flags.SetVectorMetadata();
		segment_state.InitializeSegment(buffer_collection, vectors_in_segment);
		if (!(buffer_collection.GetCurrentOffset() <= GetWritableSpace(info))) {
			throw InternalException("(ZSTDCompressionState::NewSegment) Offset (%d) exceeds writable space! (%d)",
			                        buffer_collection.GetCurrentOffset(), GetWritableSpace(info));
		}
	}

	void InitializeVector() {
		D_ASSERT(!vector_state.in_vector);
		idx_t expected_tuple_count;
		if (vector_count + 1 >= total_vector_count) {
			//! Last vector
			expected_tuple_count = analyze_state->count - (ZSTD_VECTOR_SIZE * vector_count);
		} else {
			expected_tuple_count = ZSTD_VECTOR_SIZE;
		}
		buffer_collection.AlignCurrentOffset();
		if (!(buffer_collection.GetCurrentOffset() <= GetWritableSpace(info))) {
			throw InternalException("(ZSTDCompressionState::InitializeVector) Offset (%d) exceeds writable space! (%d)",
			                        buffer_collection.GetCurrentOffset(), GetWritableSpace(info));
		}
		vector_state.compressed_size = 0;
		vector_state.uncompressed_size = 0;
		vector_state.string_lengths = nullptr;
		vector_state.tuple_count = 0;
		vector_state.vector_size = 0;
		vector_state.starting_page = 0XDEADBEEF;
		vector_state.starting_offset = 0XDEADBEEF;

		if (segment_state.vector_in_segment_count + 1 > segment_state.total_vectors_in_segment) {
			//! Last vector in the segment
			NewSegment();
		}

		if (buffer_collection.GetCurrentOffset() + (expected_tuple_count * sizeof(string_length_t)) >=
		    GetWritableSpace(info)) {
			// Check if there is room on the current page for the vector data
			NewPage();
		}

		buffer_collection.AlignCurrentOffset();
		vector_state.Initialize(expected_tuple_count, buffer_collection, info);

		// 'out_buffer' should be set to point directly after the string_lengths
		ResetOutBuffer();

		// Initialize the context for streaming compression
		duckdb_zstd::ZSTD_CCtx_reset(analyze_state->context, duckdb_zstd::ZSTD_reset_session_only);
		duckdb_zstd::ZSTD_CCtx_refCDict(analyze_state->context, nullptr);
		duckdb_zstd::ZSTD_CCtx_setParameter(analyze_state->context, duckdb_zstd::ZSTD_c_compressionLevel,
		                                    GetCompressionLevel());
		vector_state.in_vector = true;
	}

	void CompressString(const string_t &string, bool end_of_vector) {
		duckdb_zstd::ZSTD_inBuffer in_buffer = {/*data = */ string.GetData(),
		                                        /*length = */ size_t(string.GetSize()),
		                                        /*pos = */ 0};

		if (!end_of_vector && string.GetSize() == 0) {
			return;
		}
		vector_state.uncompressed_size += string.GetSize();
		const auto end_mode = end_of_vector ? duckdb_zstd::ZSTD_e_end : duckdb_zstd::ZSTD_e_continue;

		size_t compress_result;
		while (true) {
			idx_t old_pos = out_buffer.pos;

			compress_result =
			    duckdb_zstd::ZSTD_compressStream2(analyze_state->context, &out_buffer, &in_buffer, end_mode);
			D_ASSERT(out_buffer.pos >= old_pos);
			auto diff = out_buffer.pos - old_pos;
			vector_state.compressed_size += diff;
			buffer_collection.GetCurrentOffset() += diff;

			if (duckdb_zstd::ZSTD_isError(compress_result)) {
				throw InvalidInputException("ZSTD Compression failed: %s",
				                            duckdb_zstd::ZSTD_getErrorName(compress_result));
			}
			if (!(buffer_collection.GetCurrentOffset() <= GetWritableSpace(info))) {
				throw InternalException(
				    "(ZSTDCompressionState::CompressString) Offset (%d) exceeds writable space! (%d)",
				    buffer_collection.GetCurrentOffset(), GetWritableSpace(info));
			}
			if (compress_result == 0) {
				// Finished
				break;
			}
			NewPage();
		}
	}

	void AddStringInternal(const string_t &string) {
		if (!vector_state.tuple_count) {
			InitializeVector();
		}

		auto is_final_string = vector_state.AddStringLength(string);
		CompressString(string, is_final_string);
		if (is_final_string) {
			// Reached the end of this vector
			FlushVector();
		}
	}

	void AddString(const string_t &string) {
		AddStringInternal(string);
		UncompressedStringStorage::UpdateStringStats(buffer_collection.segment->stats, string);
	}

	void NewPage(bool additional_data_page = false) {
		block_id_t new_id = FinalizePage();
		block_id_t current_block_id = buffer_collection.GetCurrentId();
		GetExtraPageBuffer(current_block_id);
		buffer_collection.block_id = new_id;
		ResetOutBuffer();
	}

	block_id_t FinalizePage() {
		auto &block_manager = partial_block_manager.GetBlockManager();
		auto new_id = partial_block_manager.GetFreeBlockId();

		auto &state = buffer_collection.segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		state.RegisterBlock(block_manager, new_id);

		auto &buffer_state = buffer_collection.GetCurrentBufferState();
		buffer_state.full = true;

		// Write the new id at the end of the last page
		WriteBlockIdPointer(new_id);
		D_ASSERT(buffer_state.offset <= info.GetBlockSize());
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
		segment_state.page_ids[segment_state.vector_in_segment_count] = vector_state.starting_page;
		segment_state.page_offsets[segment_state.vector_in_segment_count] = vector_state.starting_offset;
		segment_state.compressed_sizes[segment_state.vector_in_segment_count] = vector_state.compressed_size;
		segment_state.uncompressed_sizes[segment_state.vector_in_segment_count] = vector_state.uncompressed_size;
		if (segment_state.vector_in_segment_count >= segment_state.total_vectors_in_segment) {
			throw InternalException(
			    "(ZSTDCompressionState::FlushVector) Written too many vectors (%d) to this segment! (expected: %d)",
			    segment_state.vector_in_segment_count, segment_state.total_vectors_in_segment);
		}
		vector_count++;
		segment_state.vector_in_segment_count++;
		vector_state.in_vector = false;
		buffer_collection.segment->count += vector_state.tuple_count;

		vector_state.tuple_count = 0;

		//! If the string lengths live on an overflow page, and that buffer is full
		//! then we want to flush it

		auto buffer_data = buffer_collection.GetBufferData(/*include_segment=*/true);
		ZSTDCompressionBufferCollection::Slot slot = ZSTDCompressionBufferCollection::Slot::SEGMENT;
		optional_ptr<BufferHandle> buffer_handle_ptr;
		optional_ptr<ZSTDCompressionBufferState> buffer_state_ptr;
		for (auto &buffer : buffer_data) {
			auto &buffer_state = buffer.state;
			if (buffer_state.flags.HasStringMetadata()) {
				if (buffer_handle_ptr) {
					throw InternalException("(ZSTDCompressionState::FlushVector) Multiple buffers (%d and %d) have "
					                        "string metadata on them, this is impossible and indicates a bug!",
					                        static_cast<uint8_t>(slot), static_cast<uint8_t>(buffer.slot));
				}
				slot = buffer.slot;
				buffer_state_ptr = buffer.state;
				buffer_handle_ptr = buffer.handle;
			}
			buffer_state.flags.UnsetStringMetadata();
			buffer_state.flags.UnsetData();
		}

		if (!buffer_handle_ptr) {
			throw InternalException("(ZSTDCompressionState::FlushVector) None of the buffers have string metadata on "
			                        "them, this is impossible and indicates a bug!");
		}
		if (slot == ZSTDCompressionBufferCollection::Slot::SEGMENT) {
			//! This is the segment handle, we don't need to flush that here, it'll get flushed when the segment is done
			return;
		}
		auto &buffer_state = *buffer_state_ptr;
		if (!buffer_state.full) {
			//! It contains the string metadata of the current vector, but the buffer isn't full
			//! so we don't need to flush it yet
			return;
		}
		auto &buffer_handle = *buffer_handle_ptr;
		FlushPage(buffer_handle, vector_state.starting_page);
		buffer_state.offset = 0;
		buffer_state.full = false;
	}

	void CreateEmptySegment() {
		auto &db = checkpoint_data.GetDatabase();
		auto &type = checkpoint_data.GetType();
		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, function, type, info.GetBlockSize(), info.GetBlockManager());
		buffer_collection.segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
		buffer_collection.segment_handle = buffer_manager.Pin(buffer_collection.segment->block);
	}

	void FlushSegment() {
		if (!buffer_collection.segment) {
			return;
		}
		if (segment_state.vector_in_segment_count != segment_state.total_vectors_in_segment) {
			throw InternalException("(ZSTDCompressionState::FlushSegment) We haven't written all vectors that we were "
			                        "expecting to write (%d instead of %d)!",
			                        segment_state.vector_in_segment_count, segment_state.total_vectors_in_segment);
		}

		auto &segment_buffer_state = buffer_collection.buffer_states[0];
		auto segment_block_size = segment_buffer_state.offset;
		if (segment_block_size < GetVectorMetadataSize(segment_state.total_vectors_in_segment)) {
			throw InternalException(
			    "(ZSTDCompressionState::FlushSegment) Expected offset to be at least %d, but found %d instead",
			    GetVectorMetadataSize(segment_state.total_vectors_in_segment), segment_block_size);
		}

		bool seen_dirty_buffer = false;
		auto buffer_data = buffer_collection.GetBufferData(/*include_segment=*/false);
		for (auto &buffer : buffer_data) {
			auto &buffer_state = buffer.state;
			auto &buffer_handle = buffer.handle;
			if (buffer_state.offset != 0) {
				if (seen_dirty_buffer) {
					throw InternalException("(ZSTDCompressionState::FlushSegment) Both extra pages were dirty (needed "
					                        "to be flushed), this should be impossible");
				}
				FlushPage(buffer_handle, buffer_collection.block_id);
				buffer_state.full = false;
				buffer_state.offset = 0;
				buffer_state.flags.Clear();
				seen_dirty_buffer = true;
			}
		}

		auto &state = checkpoint_data.GetCheckpointState();
		state.FlushSegment(std::move(buffer_collection.segment), std::move(buffer_collection.segment_handle),
		                   segment_block_size);
		segment_buffer_state.flags.Clear();
		segment_buffer_state.full = true;
		segment_buffer_state.offset = 0;
		segment_count++;
	}

	void Finalize() {
		D_ASSERT(!vector_state.tuple_count);
		FlushSegment();
		buffer_collection.segment.reset();
	}

	void AddNull() {
		buffer_collection.segment->stats.statistics.SetHasNullFast();
		string_t empty(static_cast<uint32_t>(0));
		AddStringInternal(empty);
	}

public:
	unique_ptr<ZSTDAnalyzeState> analyze_state;
	ColumnDataCheckpointData &checkpoint_data;
	PartialBlockManager &partial_block_manager;
	CompressionFunction &function;

	//! --- Analyzed Data ---
	//! The amount of tuples we're writing
	const idx_t total_tuple_count;
	//! The amount of vectors we're writing
	const idx_t total_vector_count;
	//! The total amount of segments we're writing
	const idx_t total_segment_count;
	//! The vectors to store in a segment (not the last one)
	const idx_t vectors_per_segment;

	//! The amount of vectors we've seen so far
	idx_t vector_count = 0;
	//! Current segment index we're at
	idx_t segment_count = 0;

	ZSTDCompressionBufferCollection buffer_collection;

	//! The compression context indicating where we are in the output buffer
	duckdb_zstd::ZSTD_outBuffer out_buffer;

	//! State of the current Vector we are writing
	ZSTDCompressionVectorState vector_state;
	//! State of the current Segment we are writing
	ZSTDCompressionSegmentState segment_state;
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
		if (!vdata.validity.RowIsValid(idx)) {
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
	      block_manager(segment.block->GetBlockManager()), buffer_manager(BufferManager::GetBufferManager(segment.db)),
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

void ZSTDStorage::VisitBlockIds(const ColumnSegment &segment, BlockIdVisitor &visitor) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	for (auto &block_id : state.on_disk_blocks) {
		visitor.Visit(block_id);
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
	zstd.visit_block_ids = ZSTDStorage::VisitBlockIds;
	return zstd;
}

bool ZSTDFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

} // namespace duckdb
