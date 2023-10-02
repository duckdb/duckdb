#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, ColumnData &parent)
    : ColumnData(block_manager, info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), &parent) {
}

bool ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return true;
}

} // namespace duckdb
