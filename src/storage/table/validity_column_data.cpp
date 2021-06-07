#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(DataTableInfo &info, idx_t column_index, idx_t start_row, ColumnData *parent)
    : ColumnData(info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), parent) {
}

bool ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return true;
}

void ValidityColumnData::InitializeScan(ColumnScanState &state) {
	state.current = (ColumnSegment *)data.GetRootSegment();
	state.row_index = state.current ? state.current->start : 0;
	state.initialized = false;
}

void ValidityColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = (ColumnSegment *)data.GetSegment(row_idx);
	state.row_index = row_idx;
	state.initialized = false;
}

} // namespace duckdb
