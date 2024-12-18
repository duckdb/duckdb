#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/validity_checkpoint_state.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, ColumnData &parent)
    : ColumnData(block_manager, info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), &parent) {
}

FilterPropagateResult ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

ValidityColumnCheckpointState::ValidityColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
                                                             PartialBlockManager &partial_block_manager,
                                                             SegmentLock &&lock)
    : ColumnCheckpointState(row_group, column_data, partial_block_manager, std::move(lock)) {
}

unique_ptr<ColumnCheckpointState> ValidityColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                            PartialBlockManager &partial_block_manager,
                                                                            SegmentLock &&lock) {
	return make_uniq<ValidityColumnCheckpointState>(row_group, *this, partial_block_manager, std::move(lock));
}

void ValidityColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	lock_guard<mutex> l(stats_lock);
	ColumnData::AppendData(stats, state, vdata, count);
}
} // namespace duckdb
