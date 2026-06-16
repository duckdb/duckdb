#include "duckdb/storage/table/row_number_column_data.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

RowNumberColumnData::RowNumberColumnData(BlockManager &block_manager, DataTableInfo &info)
    : DefaultVirtualColumnData(block_manager, info, COLUMN_IDENTIFIER_ROW_NUMBER, LogicalType(LogicalTypeId::BIGINT)) {
}

idx_t RowNumberColumnData::GetRowNumberBase(ColumnScanState &state) {
	return state.parent->row_number_base.GetIndex();
}

FilterPropagateResult RowNumberColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// row_number columns don't have zonemaps - we cannot prune based on row number
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

void RowNumberColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
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
	state.offset_in_column = 0;
	state.internal_index = 0;
	state.initialized = true;
	state.scan_state.reset();
	state.last_offset = 0;
}

idx_t RowNumberColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                                idx_t scan_count) {
	return ScanCount(state, result, scan_count, 0);
}

idx_t RowNumberColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count, idx_t result_offset) {
	if (result_offset != 0) {
		throw InternalException("RowNumberColumnData result_offset must be 0");
	}
	auto base = GetRowNumberBase(state);
	// row_number is 1-indexed
	result.Sequence(NumericCast<int64_t>(base + state.offset_in_column + 1), 1, count);
	state.offset_in_column += count;
	return count;
}

void RowNumberColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state,
                                 Vector &result, SelectionVector &sel, idx_t count) {
	auto base = GetRowNumberBase(state);
	// row_number is a dense sequence - deleted/filtered rows don't get numbers
	// 1-indexed
	result.Sequence(NumericCast<int64_t>(base + state.offset_in_column + 1), 1, count);
	state.offset_in_column += count;
}

void RowNumberColumnData::Skip(ColumnScanState &state, idx_t count) {
	// row_number is dense - skipped rows don't consume row numbers
	// so Skip is a no-op (offset_in_column is not advanced)
}

string RowNumberColumnData::GetColumnDataName() const {
	return "RowNumberColumnData";
}

} // namespace duckdb
