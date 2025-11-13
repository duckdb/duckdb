#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"

namespace duckdb {

	ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
						   ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, LogicalType(LogicalTypeId::VALIDITY), data_type,
                 parent) {
}

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
					   ColumnData &parent) :
	ValidityColumnData(block_manager, info, column_index, parent.GetDataType(), parent){}

FilterPropagateResult ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

void ValidityColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	lock_guard<mutex> l(stats_lock);
	ColumnData::AppendData(stats, state, vdata, count);
}
} // namespace duckdb
