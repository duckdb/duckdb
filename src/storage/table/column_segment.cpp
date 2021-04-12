#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/storage/table/update_segment.hpp"

#include <cstring>

namespace duckdb {

ColumnSegment::ColumnSegment(DatabaseInstance &db, LogicalType type_p, ColumnSegmentType segment_type, idx_t start, idx_t count)
    : SegmentBase(start, count), db(db), type(move(type_p)), type_size(GetTypeIdSize(type.InternalType())),
	segment_type(segment_type), stats(type, type_size) {
}

ColumnSegment::ColumnSegment(DatabaseInstance &db, LogicalType type_p, ColumnSegmentType segment_type, idx_t start, idx_t count,
                             unique_ptr<BaseStatistics> statistics)
    : SegmentBase(start, count), db(db), type(move(type_p)), type_size(GetTypeIdSize(type.InternalType())),
	segment_type(segment_type), stats(type, type_size, move(statistics)) {
}

ColumnSegment::~ColumnSegment() {}

void ColumnSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t start_row, idx_t scan_count, Vector &result, idx_t result_offset) {
	D_ASSERT(start_row + scan_count <= this->count);
	data->Scan(state, start_row, scan_count, result, result_offset);
}

void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	data->FetchRow(state, row_id - this->start, result, result_idx);
}

void ColumnSegment::MergeUpdates(Transaction &transaction, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	if (!updates) {
		return;
	}
	throw NotImplementedException("FIXME: merge updates");
}

void ColumnSegment::MergeCommitted(idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	if (!updates) {
		return;
	}
	throw NotImplementedException("FIXME: merge committed");
}

void ColumnSegment::MergeRowUpdate(Transaction &transaction, row_t row_id, Vector &result, idx_t result_idx) {
	if (!updates) {
		return;
	}
	throw NotImplementedException("FIXME: merge row update");
	// updates->FetchRow(transaction, row_id, result, result_idx);
}

} // namespace duckdb
