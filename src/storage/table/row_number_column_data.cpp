#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_number_column_data.hpp"

namespace duckdb {

RowNumberColumnData::RowNumberColumnData(BlockManager &block_manager, DataTableInfo &info)
    : ColumnData(block_manager, info, COLUMN_IDENTIFIER_ROW_NUMBER, LogicalType(LogicalTypeId::BIGINT),
                 ColumnDataType::MAIN_TABLE, nullptr) {
}

void RowNumberColumnData::InitializeScan(ColumnScanState &state) {
	InitializeScanWithOffset(state, 0);
}

void RowNumberColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx > count) {
		throw InternalException("row_idx in InitializeScanWithOffset out of range");
	}
	state.current = nullptr;
	state.segment_tree = nullptr;
	state.offset_in_column = row_idx;
	state.internal_index = state.offset_in_column;
	state.initialized = true;
	state.scan_state.reset();
	state.last_offset = 0;
}

idx_t RowNumberColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                                idx_t scan_count) {
	return ScanCommitted(vector_index, state, result, false, scan_count);
}

idx_t RowNumberColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates,
                                         idx_t scan_count) {
	return ScanCount(state, result, scan_count, 0);
}

void RowNumberColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count,
                                             Vector &result) {
	result.Sequence(UnsafeNumericCast<int64_t>(1 + offset_in_row_group), 1, count);
}

idx_t RowNumberColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	auto row_start = state.parent->row_group->row_start;
	if (result_offset != 0) {
		throw InternalException("RowNumberColumnData result_offset must be 0");
	}
	ScanCommittedRange(row_start, state.offset_in_column, count, result);
	state.offset_in_column += count;
	return count;
}

} // namespace duckdb
