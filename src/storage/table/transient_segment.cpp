#include "storage/table/transient_segment.hpp"

using namespace duckdb;
using namespace std;

TransientSegment::TransientSegment(index_t start)
    : ColumnSegment(ColumnSegmentType::TRANSIENT, start, 0), block(INVALID_BLOCK) {

}
