#include "storage/table/column_segment.hpp"

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, index_t start)
    : SegmentBase(start, 0), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type) {
}
