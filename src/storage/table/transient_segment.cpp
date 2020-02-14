#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"

using namespace duckdb;
using namespace std;

TransientSegment::TransientSegment(BufferManager &manager, TypeId type, index_t start)
    : ColumnSegment(type, ColumnSegmentType::TRANSIENT, start), manager(manager) {
	if (type == TypeId::VARCHAR) {
		data = make_unique<StringSegment>(manager, start);
	} else {
		data = make_unique<NumericSegment>(manager, type, start);
	}
}

void TransientSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void TransientSegment::Scan(Transaction &transaction, ColumnScanState &state, index_t vector_index, Vector &result) {
	data->Scan(transaction, state, vector_index, result);
}

void TransientSegment::IndexScan(ColumnScanState &state, Vector &result) {
	data->IndexScan(state, state.vector_index, result);
}

void TransientSegment::Fetch(ColumnScanState &state, index_t vector_index, Vector &result) {
	data->Fetch(state, vector_index, result);
}

void TransientSegment::FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result, index_t result_idx) {
	data->FetchRow(state, transaction, row_id - this->start, result, result_idx);
}

void TransientSegment::Update(ColumnData &column_data, Transaction &transaction, Vector &updates, row_t *ids) {
	data->Update(column_data, stats, transaction, updates, ids, this->start);
}

void TransientSegment::InitializeAppend(ColumnAppendState &state) {
	state.lock = data->lock.GetExclusiveLock();
}

index_t TransientSegment::Append(ColumnAppendState &state, Vector &append_data, index_t offset, index_t count) {
	index_t appended = data->Append(stats, append_data, offset, count);
	this->count += appended;
	return appended;
}

void TransientSegment::RevertAppend(index_t start_row) {
	data->tuple_count = start_row - this->start;
	this->count = start_row - this->start;
}
