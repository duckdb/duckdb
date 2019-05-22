#include "storage/column_segment.hpp"

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(BlockManager *manager, block_id_t block_id, index_t offset, count_t count, index_t start) :
	SegmentBase(start, count), block_id(block_id), offset(offset), manager(manager) {
}

ColumnSegment::ColumnSegment(index_t start) :
	SegmentBase(start, 0), block_id(INVALID_BLOCK), offset(0), manager(nullptr) {
}

data_ptr_t ColumnSegment::GetData() {
	// FIXME: we can do better than a mutex every time this pointer is obtained
	if (!block) {
		lock_guard<mutex> lock(data_lock);
		if (!block) {
			block = make_unique<Block>(block_id);
			// no block yet: have to load the block
			if (block_id != INVALID_BLOCK) {
				// block is not an in-memory block: load the contents from disk
				manager->Read(*block);
			}
		}
	}
	return block->buffer + offset;
}
