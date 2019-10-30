#include "storage/table/transient_segment.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "storage/numeric_segment.hpp"
#include "storage/string_segment.hpp"

using namespace duckdb;
using namespace std;

TransientSegment::TransientSegment(BufferManager &manager, TypeId type, index_t start)
    : ColumnSegment(type, ColumnSegmentType::TRANSIENT, start), manager(manager) {
	if (type == TypeId::VARCHAR) {
		data = make_unique<StringSegment>(manager);
	} else {
		data = make_unique<NumericSegment>(manager, type);
	}
}

void TransientSegment::InitializeScan(TransientScanState &state) {
	data->InitializeScan(state);
}

void TransientSegment::Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result) {
	data->Scan(transaction, state, vector_index, result);
}

void TransientSegment::IndexScan(TransientScanState &state, Vector &result) {
	data->IndexScan(state, state.vector_index, result);
}

void TransientSegment::InitializeAppend(TransientAppendState &state) {
	state.lock = data->lock.GetExclusiveLock();
}

index_t TransientSegment::Append(TransientAppendState &state, Vector &append_data, index_t offset, index_t count) {
	index_t appended = data->Append(stats, append_data, offset, count);
	this->count += appended;
	return appended;
}

void TransientSegment::Update(DataTable &table, Transaction &transaction, Vector &updates, row_t *ids) {
	data->Update(table, stats, transaction, updates, ids, this->start);
}

void TransientSegment::Fetch(TransientScanState &state, index_t vector_index, Vector &result) {
	data->Fetch(state, vector_index, result);
}

void TransientSegment::FetchRow(FetchState &state, Transaction &transaction, row_t row_id, Vector &result) {
	data->FetchRow(state, transaction, row_id - this->start, result);
}
