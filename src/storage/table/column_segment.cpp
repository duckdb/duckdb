#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/limits.hpp"

#include <cstring>

namespace duckdb {
using namespace std;

ColumnSegment::ColumnSegment(LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type.InternalType())), segment_type(segment_type),
      stats(type, type_size) {
}

ColumnSegment::ColumnSegment(LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count,
                             unique_ptr<BaseStatistics> statistics)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type.InternalType())), segment_type(segment_type),
      stats(type, type_size, move(statistics)) {
}

} // namespace duckdb
