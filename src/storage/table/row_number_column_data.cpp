#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_number_column_data.hpp"

namespace duckdb {

RowNumberColumnData::RowNumberColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t start_row)
    : ColumnData(block_manager, info, COLUMN_IDENTIFIER_ROW_NUMBER, start_row, LogicalType(LogicalTypeId::BIGINT),
                 nullptr) {
}

void RowNumberColumnData::InitializeScan(ColumnScanState &state) {
	InitializeScanWithOffset(state, start);
}

void RowNumberColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.row_index = row_idx;
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
	D_ASSERT(this->start == row_group_start);
	result.Sequence(UnsafeNumericCast<int64_t>(1 + offset_in_row_group), 1, count);
}

idx_t RowNumberColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	if (result_offset != 0) {
		throw InternalException("RowNumberColumnData result_offset must be 0");
	}
	ScanCommittedRange(start, state.row_index - start, count, result);
	state.row_index += count;
	return count;
}

} // namespace duckdb
