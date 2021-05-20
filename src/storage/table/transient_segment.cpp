#include "duckdb/storage/table/transient_segment.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/string_segment.hpp"
#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

TransientSegment::TransientSegment(DatabaseInstance &db, const LogicalType &type_p, idx_t start)
    : ColumnSegment(type_p, ColumnSegmentType::TRANSIENT, start), db(db) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data = make_unique<StringSegment>(db, start);
	} else if (type.InternalType() == PhysicalType::BIT) {
		data = make_unique<ValiditySegment>(db, start);
	} else {
		data = make_unique<NumericSegment>(db, type.InternalType(), start);
	}
}

TransientSegment::TransientSegment(PersistentSegment &segment)
    : ColumnSegment(segment.type, ColumnSegmentType::TRANSIENT, segment.start), db(segment.db) {
	if (segment.block_id == segment.data->block->BlockId()) {
		segment.data->ToTemporary();
	}
	data = move(segment.data);
	stats = move(segment.stats);
	count = segment.count.load();
	D_ASSERT(!segment.next);
}

void TransientSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void TransientSegment::Scan(ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Scan(state, vector_index, result);
}

void TransientSegment::Fetch(ColumnScanState &state, idx_t vector_index, Vector &result) {
	data->Fetch(state, vector_index, result);
}

void TransientSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	data->FetchRow(state, row_id - this->start, result, result_idx);
}

void TransientSegment::InitializeAppend(ColumnAppendState &state) {
}

idx_t TransientSegment::Append(ColumnAppendState &state, VectorData &append_data, idx_t offset, idx_t count) {
	idx_t appended = data->Append(stats, append_data, offset, count);
	this->count += appended;
	return appended;
}

void TransientSegment::RevertAppend(idx_t start_row) {
	data->RevertAppend(start_row);
	this->count = start_row - this->start;
}

} // namespace duckdb
