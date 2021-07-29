#include "duckdb/storage/segment/uncompressed_segment.hpp"

namespace duckdb {

UncompressedSegment::UncompressedSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start)
    : BaseSegment(db, type, row_start) {
}

UncompressedSegment::~UncompressedSegment() {
}

void UncompressedSegment::RevertAppend(idx_t start_row) {
	tuple_count = start_row - this->row_start;
}

} // namespace duckdb
