#pragma once

#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/string_checkpoint_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {
struct StringDictionaryContainer {
	//! The size of the dictionary
	uint32_t size;
	//! The end of the dictionary (typically Storage::BLOCK_SIZE)
	uint32_t end;

	void Verify() {
		D_ASSERT(size <= Storage::BLOCK_SIZE);
		D_ASSERT(end <= Storage::BLOCK_SIZE);
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
	//! The size below which the segment is compacted on flushing
	static constexpr size_t COMPACTION_FLUSH_LIMIT = (size_t)Storage::BLOCK_SIZE / 5 * 4;

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
	static unique_ptr<CompressedSegmentState> StringInitSegment(ColumnSegment &segment, block_id_t block_id);

	static idx_t StringAppend(ColumnSegment &segment, SegmentStatistics &stats, UnifiedVectorFormat &data, idx_t offset,
	                          idx_t count) {
		return StringAppendBase(segment, stats, data, offset, count);
	}

	template <bool DUPLICATE_ELIMINATE = false>
	static idx_t StringAppendBase(ColumnSegment &segment, SegmentStatistics &stats, UnifiedVectorFormat &data,
	                              idx_t offset, idx_t count,
	                              std::unordered_map<string, int32_t> *seen_strings = nullptr) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);

		D_ASSERT(segment.GetBlockOffset() == 0);
		auto source_data = (string_t *)data.data;
		auto result_data = (int32_t *)(handle.Ptr() + DICTIONARY_HEADER_SIZE);
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = data.sel->get_index(offset + i);
			auto target_idx = segment.count.load();
			idx_t remaining_space = RemainingSpace(segment, handle);
			if (remaining_space < sizeof(int32_t)) {
				// string index does not fit in the block at all
				return i;
			}
			remaining_space -= sizeof(int32_t);
			auto dictionary = GetDictionary(segment, handle);
			if (!data.validity.RowIsValid(source_idx)) {
				// null value is stored as a copy of the last value, this is done to be able to efficiently do the
				// string_length calculation
				if (target_idx > 0) {
					result_data[target_idx] = result_data[target_idx - 1];
				} else {
					result_data[target_idx] = 0;
				}
			} else {
				auto end = handle.Ptr() + dictionary.end;

				dictionary.Verify();

				int32_t match;
				bool found;
				if (DUPLICATE_ELIMINATE) {
					auto search = seen_strings->find(source_data[source_idx].GetString());
					if (search != seen_strings->end()) {
						match = search->second;
						found = true;
					} else {
						found = false;
					}
				}

				if (DUPLICATE_ELIMINATE && found) {
					// We have seen this string
					result_data[target_idx] = match;
				} else {
					// Unknown string, continue
					// non-null value, check if we can fit it within the block
					idx_t string_length = source_data[source_idx].GetSize();
					idx_t dictionary_length = string_length;

					// determine whether or not we have space in the block for this string
					bool use_overflow_block = false;
					idx_t required_space = dictionary_length;
					if (required_space >= StringUncompressed::STRING_BLOCK_LIMIT) {
						// string exceeds block limit, store in overflow block and only write a marker here
						required_space = BIG_STRING_MARKER_SIZE;
						use_overflow_block = true;
					}
					if (required_space > remaining_space) {
						// no space remaining: return how many tuples we ended up writing
						return i;
					}

					// we have space: write the string
					UpdateStringStats(stats, source_data[source_idx]);

					if (use_overflow_block) {
						// write to overflow blocks
						block_id_t block;
						int32_t offset;
						// write the string into the current string block
						WriteString(segment, source_data[source_idx], block, offset);
						dictionary.size += BIG_STRING_MARKER_SIZE;
						auto dict_pos = end - dictionary.size;

						// write a big string marker into the dictionary
						WriteStringMarker(dict_pos, block, offset);
					} else {
						// string fits in block, append to dictionary and increment dictionary position
						D_ASSERT(string_length < NumericLimits<uint16_t>::Maximum());
						dictionary.size += required_space;
						auto dict_pos = end - dictionary.size;
						// now write the actual string data into the dictionary
						memcpy(dict_pos, source_data[source_idx].GetDataUnsafe(), string_length);
					}
					D_ASSERT(RemainingSpace(segment, handle) <= Storage::BLOCK_SIZE);
					// place the dictionary offset into the set of vectors
					dictionary.Verify();

					// note: for overflow strings we write negative value
					result_data[target_idx] = use_overflow_block ? -1 * dictionary.size : dictionary.size;

					if (DUPLICATE_ELIMINATE) {
						seen_strings->insert({source_data[source_idx].GetString(), dictionary.size});
					}
					SetDictionary(segment, handle, dictionary);
				}
			}
			segment.count++;
		}
		return count;
	}

	static idx_t FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats);

public:
	static inline void UpdateStringStats(SegmentStatistics &stats, const string_t &new_value) {
		auto &sstats = (StringStatistics &)*stats.statistics;
		sstats.Update(new_value);
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

	static string_location_t FetchStringLocation(StringDictionaryContainer dict, data_ptr_t baseptr,
	                                             int32_t dict_offset);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                                    data_ptr_t baseptr, int32_t dict_offset, uint32_t string_length);
	static string_t FetchString(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                            data_ptr_t baseptr, string_location_t location, uint32_t string_length);
};
} // namespace duckdb
