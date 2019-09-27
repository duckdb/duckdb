#include "storage/table/transient_segment.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

TransientSegment::TransientSegment(BufferManager &manager, TypeId type, index_t start)
    : ColumnSegment(type, ColumnSegmentType::TRANSIENT, start), manager(manager), data(manager, type) {
}

void TransientSegment::InitializeScan(TransientScanState &state) {
	// initialize the uncompressed segment scan state
	// data.InitializeScan(state.state);
}

void TransientSegment::Scan(Transaction &transaction, index_t vector_index, Vector &result) {
	data.Scan(transaction, vector_index, result);
}

void TransientSegment::IndexScan(TransientScanState &state, Vector &result) {
	data.IndexScan(state.state, state.vector_index, result);
}

void TransientSegment::InitializeAppend(TransientAppendState &state) {
	state.lock = data.lock.GetExclusiveLock();
	state.handle = manager.PinBuffer(data.block_id);
}

index_t TransientSegment::Append(TransientAppendState &state, Vector &append_data, index_t offset, index_t count) {
	index_t appended = data.Append(stats, state, append_data, offset, count);
	this->count += appended;
	return appended;
}

void TransientSegment::Update(Transaction &transaction, Vector &updates, row_t *ids) {
	data.Update(stats, transaction, updates, ids, this->start);
}

void TransientSegment::Fetch(index_t vector_index, Vector &result) {
	data.Fetch(vector_index, result);
}

void TransientSegment::FetchRow(Transaction &transaction, row_t row_id, Vector &result) {
	data.Fetch(transaction, row_id - this->start, result);
}
