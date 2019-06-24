#include "storage/table/column_segment.hpp"

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(ColumnSegmentType type, index_t start, index_t count) :
	SegmentBase(start, count), type(type) {

}
