#include "storage/table/column_segment.hpp"

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(TypeId type, ColumnSegmentType segment_type, index_t start, index_t count)
    : SegmentBase(start, count), type(type), type_size(GetTypeIdSize(type)), segment_type(segment_type),
      stats(type, type_size) {
}

SegmentStatistics::SegmentStatistics(TypeId type, index_t type_size) {
	minimum = unique_ptr<data_t[]>(new data_t[type_size]);
	maximum = unique_ptr<data_t[]>(new data_t[type_size]);
	has_null = false;
}
