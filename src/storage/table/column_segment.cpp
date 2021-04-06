#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/limits.hpp"

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

void ColumnSegment::InitializeScan(ColumnScanState &state) {
	data->InitializeScan(state);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t row_index, Vector &result) {
	D_ASSERT(row_index >= start && row_index < start + count);
	data->Scan(state, row_index - start, result);
}

void ColumnSegment::Fetch(ColumnScanState &state, idx_t row_index, Vector &result) {
	D_ASSERT(row_index >= start && row_index < start + count);
	data->Fetch(state, row_index - start, result);
}

void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	data->FetchRow(state, row_id - this->start, result, result_idx);
}

} // namespace duckdb
