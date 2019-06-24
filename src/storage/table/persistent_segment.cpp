#include "storage/table/persistent_segment.hpp"

using namespace duckdb;
using namespace std;

PersistentSegment::PersistentSegment(BlockManager &manager, block_id_t block_id, index_t offset, index_t count, index_t start) :
    ColumnSegment(ColumnSegmentType::PERSISTENT, start, count), manager(manager), block_id(block_id), offset(offset) {
}
