#include "duckdb/storage/table/validity_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table/standard_column_data.hpp"

namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, ColumnData &parent)
    : ColumnData(block_manager, info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), &parent) {
}

FilterPropagateResult ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

void ValidityColumnData::UpdateWithBase(TransactionData transaction, DataTable &data_table, idx_t column_index,
                                        Vector &update_vector, row_t *row_ids, idx_t update_count, ColumnData &base) {
	Vector base_vector(base.type);
	ColumnScanState validity_scan_state;
	FetchUpdateData(validity_scan_state, row_ids, base_vector);

	if (validity_scan_state.current->GetCompressionFunction().type == CompressionType::COMPRESSION_EMPTY) {
		// The validity is actually covered by the data, so we read it to get the validity for UpdateInternal.
		ColumnScanState data_scan_state;
		auto fetch_count = base.Fetch(data_scan_state, row_ids[0], base_vector);
		base_vector.Flatten(fetch_count);
	}

	UpdateInternal(transaction, data_table, column_index, update_vector, row_ids, update_count, base_vector);
}

void ValidityColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	lock_guard<mutex> l(stats_lock);
	ColumnData::AppendData(stats, state, vdata, count);
}
} // namespace duckdb
