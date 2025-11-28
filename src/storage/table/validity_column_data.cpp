#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, LogicalType(LogicalTypeId::VALIDITY), data_type, parent) {
}

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       ColumnData &parent)
    : ValidityColumnData(block_manager, info, column_index, parent.GetDataType(), parent) {
}

FilterPropagateResult ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

void ValidityColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	lock_guard<mutex> l(stats_lock);
	ColumnData::AppendData(stats, state, vdata, count);
}

struct ValidityColumnCheckpointState : public ColumnCheckpointState {
	ValidityColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data,
	                              PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
	}

public:
	shared_ptr<ColumnData> CreateEmptyColumnData() override {
		return make_shared_ptr<ValidityColumnData>(original_column.GetBlockManager(), original_column.GetTableInfo(),
		                                           original_column.column_index, ColumnDataType::CHECKPOINT_TARGET,
		                                           nullptr);
	}
};

unique_ptr<ColumnCheckpointState>
ValidityColumnData::CreateCheckpointState(const RowGroup &row_group, PartialBlockManager &partial_block_manager) {
	return make_uniq<ValidityColumnCheckpointState>(row_group, *this, partial_block_manager);
}

void ValidityColumnData::Verify(RowGroup &parent) {
	D_ASSERT(HasParent());
	ColumnData::Verify(parent);
}

} // namespace duckdb
