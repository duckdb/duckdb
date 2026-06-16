#include "duckdb/storage/table/row_is_present_column_data.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

RowIsPresentColumnData::RowIsPresentColumnData(BlockManager &block_manager, DataTableInfo &info)
    : DefaultVirtualColumnData(block_manager, info, COLUMN_IDENTIFIER_ROW_IS_PRESENT,
                               LogicalType(LogicalTypeId::BOOLEAN)) {
}

void RowIsPresentColumnData::InitializePrefetch(PrefetchState &, ColumnScanState &, idx_t) {
}

void RowIsPresentColumnData::InitializeScan(ColumnScanState &state) {
	InitializeScanWithOffset(state, 0);
}

void RowIsPresentColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
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

idx_t RowIsPresentColumnData::Scan(TransactionData, idx_t, ColumnScanState &state, Vector &result, idx_t scan_count) {
	return ScanCount(state, result, scan_count, 0);
}

void RowIsPresentColumnData::ScanCommittedRange(idx_t, idx_t, idx_t count, Vector &result) {
	result.Reference(Value::BOOLEAN(true), count_t(count));
}

idx_t RowIsPresentColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	if (result_offset != 0) {
		throw InternalException("RowIsPresentColumnData result_offset must be 0");
	}
	result.Reference(Value::BOOLEAN(true), count_t(count));
	state.offset_in_column += count;
	return count;
}

void RowIsPresentColumnData::Select(TransactionData, idx_t vector_index, ColumnScanState &state, Vector &result,
                                    SelectionVector &, idx_t count) {
	result.Reference(Value::BOOLEAN(true), count_t(count));
	state.offset_in_column += GetVectorCount(vector_index);
}

void RowIsPresentColumnData::FetchRows(TransactionData, ColumnFetchState &, const StorageIndex &, const idx_t *,
                                       const SelectionVector &, idx_t count, Vector &result, idx_t result_offset) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto data = FlatVector::GetDataMutable<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		data[result_offset + i] = true;
	}
}

void RowIsPresentColumnData::Skip(ColumnScanState &state, idx_t count) {
	state.offset_in_column += count;
	state.internal_index = state.offset_in_column;
}

FilterPropagateResult RowIsPresentColumnData::CheckZonemap(ColumnScanState &, TableFilter &) {
	// always true — no zonemap, nothing to prune on
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string RowIsPresentColumnData::GetColumnDataName() const {
	return "RowIsPresentColumnData";
}

} // namespace duckdb
