#pragma once

#include "duckdb/storage/statistics/string_statistics.hpp"

#include <duckdb.h>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>

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

struct UncompressedStringStorage {
public:
	//! Dictionary header size at the beginning of the string segment (offset + length)
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(uint32_t) + sizeof(uint32_t);
	//! Marker used in length field to indicate the presence of a big string
	static constexpr uint16_t BIG_STRING_MARKER = (uint16_t)-1;
	//! Base size of big string marker (block id + offset)
	static constexpr idx_t BIG_STRING_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
	//! The marker size of the big string
	static constexpr idx_t BIG_STRING_MARKER_SIZE = BIG_STRING_MARKER_BASE_SIZE + sizeof(uint16_t);

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
	static idx_t StringAppend(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset,
	                          idx_t count);
	static idx_t StringAppendBase(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset,
	                              idx_t count, std::map<string, int32_t> *seen_strings);
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
	static string_t ReadString(ColumnSegment &segment, Vector &result, block_id_t block, int32_t offset);
	static string_t ReadString(data_ptr_t target, int32_t offset);
	static void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset);
	static void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset);

	static string_location_t FetchStringLocation(StringDictionaryContainer dict, data_ptr_t baseptr,
	                                             int32_t dict_offset);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                                    data_ptr_t baseptr, int32_t dict_offset);
	static string_t FetchString(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
	                            data_ptr_t baseptr, string_location_t location);
};
} // namespace duckdb