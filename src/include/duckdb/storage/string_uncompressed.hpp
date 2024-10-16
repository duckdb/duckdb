//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/string_uncompressed.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/likely.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/string_checkpoint_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {
struct StringDictionaryContainer {
	//! The size of the dictionary
	uint32_t size;
	//! The end of the dictionary, which defaults to the block size.
	uint32_t end;

	void Verify(const idx_t block_size) {
		D_ASSERT(size <= block_size);
		D_ASSERT(end <= block_size);
		D_ASSERT(size <= end);
	}
};

struct StringScanState : public SegmentScanState {
	BufferHandle handle;
};

struct UncompressedStringStorage {
public:
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
	//! Marker used in length field to indicate the presence of a big string
	static constexpr uint16_t BIG_STRING_MARKER = (uint16_t)-1;
	//! Base size of big string marker (block id + offset)
	static constexpr idx_t BIG_STRING_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
	//! The marker size of the big string
	static constexpr idx_t BIG_STRING_MARKER_SIZE = BIG_STRING_MARKER_BASE_SIZE;

public:
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);
	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
	static unique_ptr<CompressedSegmentState> StringInitSegment(ColumnSegment &segment, block_id_t block_id,
	                                                            optional_ptr<ColumnSegmentState> segment_state);

	static unique_ptr<CompressionAppendState> StringInitAppend(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		// This block was initialized in StringInitSegment
		auto handle = buffer_manager.Pin(segment.block);
		return make_uniq<CompressionAppendState>(std::move(handle));
	}

	static idx_t StringAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
	                          UnifiedVectorFormat &data, idx_t offset, idx_t count) {
		return StringAppendBase(append_state.handle, segment, stats, data, offset, count);
	}

	static idx_t StringAppendBase(ColumnSegment &segment, SegmentStatistics &stats, UnifiedVectorFormat &data,
	                              idx_t offset, idx_t count) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);
		return StringAppendBase(handle, segment, stats, data, offset, count);
	}

	static idx_t StringAppendBase(BufferHandle &handle, ColumnSegment &segment, SegmentStatistics &stats,
	                              UnifiedVectorFormat &data, idx_t offset, idx_t count) {
		D_ASSERT(segment.GetBlockOffset() == 0);
		auto handle_ptr = handle.Ptr();
		auto source_data = UnifiedVectorFormat::GetData<string_t>(data);
		auto result_data = reinterpret_cast<int32_t *>(handle_ptr + DICTIONARY_HEADER_SIZE);
		auto dictionary_size = reinterpret_cast<uint32_t *>(handle_ptr);
		auto dictionary_end = reinterpret_cast<uint32_t *>(handle_ptr + sizeof(uint32_t));

		idx_t remaining_space = RemainingSpace(segment, handle);
		auto base_count = segment.count.load();
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = data.sel->get_index(offset + i);
			auto target_idx = base_count + i;
			if (remaining_space < sizeof(int32_t)) {
				// string index does not fit in the block at all
				segment.count += i;
				return i;
			}
			remaining_space -= sizeof(int32_t);
			if (!data.validity.RowIsValid(source_idx)) {
				// null value is stored as a copy of the last value, this is done to be able to efficiently do the
				// string_length calculation
				if (target_idx > 0) {
					result_data[target_idx] = result_data[target_idx - 1];
				} else {
					result_data[target_idx] = 0;
				}
				continue;
			}
			auto end = handle.Ptr() + *dictionary_end;

#ifdef DEBUG
			GetDictionary(segment, handle).Verify(segment.GetBlockManager().GetBlockSize());
#endif
			// Unknown string, continue
			// non-null value, check if we can fit it within the block
			idx_t string_length = source_data[source_idx].GetSize();

			// determine whether or not we have space in the block for this string
			bool use_overflow_block = false;
			idx_t required_space = string_length;
			if (DUCKDB_UNLIKELY(required_space >=
			                    StringUncompressed::GetStringBlockLimit(segment.GetBlockManager().GetBlockSize()))) {
				// string exceeds block limit, store in overflow block and only write a marker here
				required_space = BIG_STRING_MARKER_SIZE;
				use_overflow_block = true;
			}
			if (DUCKDB_UNLIKELY(required_space > remaining_space)) {
				// no space remaining: return how many tuples we ended up writing
				segment.count += i;
				return i;
			}

			// we have space: write the string
			UpdateStringStats(stats, source_data[source_idx]);

			if (DUCKDB_UNLIKELY(use_overflow_block)) {
				// write to overflow blocks
				block_id_t block;
				int32_t current_offset;
				// write the string into the current string block
				WriteString(segment, source_data[source_idx], block, current_offset);
				*dictionary_size += BIG_STRING_MARKER_SIZE;
				remaining_space -= BIG_STRING_MARKER_SIZE;
				auto dict_pos = end - *dictionary_size;

				// write a big string marker into the dictionary
				WriteStringMarker(dict_pos, block, current_offset);

				// place the dictionary offset into the set of vectors
				// note: for overflow strings we write negative value

				// dictionary_size is an uint32_t value, so we can cast up.
				D_ASSERT(NumericCast<idx_t>(*dictionary_size) <= segment.GetBlockManager().GetBlockSize());
				result_data[target_idx] = -NumericCast<int32_t>((*dictionary_size));
			} else {
				// string fits in block, append to dictionary and increment dictionary position
				D_ASSERT(string_length < NumericLimits<uint16_t>::Maximum());
				*dictionary_size += required_space;
				remaining_space -= required_space;
				auto dict_pos = end - *dictionary_size;
				// now write the actual string data into the dictionary
				memcpy(dict_pos, source_data[source_idx].GetData(), string_length);

				// dictionary_size is an uint32_t value, so we can cast up.
				D_ASSERT(NumericCast<idx_t>(*dictionary_size) <= segment.GetBlockManager().GetBlockSize());
				// Place the dictionary offset into the set of vectors.
				result_data[target_idx] = NumericCast<int32_t>(*dictionary_size);
			}
			D_ASSERT(RemainingSpace(segment, handle) <= segment.GetBlockManager().GetBlockSize());
#ifdef DEBUG
			GetDictionary(segment, handle).Verify(segment.GetBlockManager().GetBlockSize());
#endif
		}
		segment.count += count;
		return count;
	}

	static idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats);

public:
	static inline void UpdateStringStats(SegmentStatistics &stats, const string_t &new_value) {
		StringStats::Update(stats.statistics, new_value);
	}

	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer dict);
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static idx_t RemainingSpace(ColumnSegment &segment, BufferHandle &handle);
	static void WriteString(ColumnSegment &segment, string_t string, block_id_t &result_block, int32_t &result_offset);
	static void WriteStringMemory(ColumnSegment &segment, string_t string, block_id_t &result_block,
	                              int32_t &result_offset);
	static string_t ReadOverflowString(ColumnSegment &segment, Vector &result, block_id_t block, int32_t offset);
	static string_t ReadString(data_ptr_t target, int32_t offset, uint32_t string_length);
	static string_t ReadStringWithLength(data_ptr_t target, int32_t offset);
	static void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset);
	static void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset);

	static string_location_t FetchStringLocation(StringDictionaryContainer dict, data_ptr_t base_ptr,
	                                             int32_t dict_offset, const idx_t block_size);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                                    data_ptr_t base_ptr, int32_t dict_offset, uint32_t string_length);
	static string_t FetchString(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                            data_ptr_t baseptr, string_location_t location, uint32_t string_length);

	static unique_ptr<ColumnSegmentState> SerializeState(ColumnSegment &segment);
	static unique_ptr<ColumnSegmentState> DeserializeState(Deserializer &deserializer);
	static void CleanupState(ColumnSegment &segment);
};
} // namespace duckdb
