#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

TransientSegment::TransientSegment(DatabaseInstance &db, const LogicalType &type_p, idx_t start)
    : ColumnSegment(type_p, ColumnSegmentType::TRANSIENT, start), db(db) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data = make_unique<StringSegment>(db, start);
	} else {
		data = make_unique<NumericSegment>(db, type.InternalType(), start);
	}
}

void TransientSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void TransientSegment::Scan(Transaction &transaction, ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Scan(transaction, state, vector_index, result);
}

void TransientSegment::FilterScan(Transaction &transaction, ColumnScanState &state, Vector &result,
                                  SelectionVector &sel, idx_t &approved_tuple_count) {
	data->FilterScan(transaction, state, result, sel, approved_tuple_count);
}

void TransientSegment::Select(Transaction &transaction, ColumnScanState &state, Vector &result, SelectionVector &sel,
                              idx_t &approved_tuple_count, vector<TableFilter> &table_filter) {
	return data->Select(transaction, result, table_filter, sel, approved_tuple_count, state);
}

void TransientSegment::Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Fetch(state, vector_index, result);
}

void TransientSegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result,
                                idx_t result_idx) {
	data->FetchRow(state, transaction, row_id - this->start, result, result_idx);
}

void TransientSegment::InitializeAppend(ColumnAppendState &state) {
}

idx_t TransientSegment::Append(ColumnAppendState &state, Vector &append_data, idx_t offset, idx_t count) {
	idx_t appended = data->Append(stats, append_data, offset, count);
	this->count += appended;
	return appended;
}

void TransientSegment::RevertAppend(idx_t start_row) {
	data->tuple_count = start_row - this->start;
	this->count = start_row - this->start;
}

} // namespace duckdb
