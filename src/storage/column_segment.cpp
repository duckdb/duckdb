#include "storage/column_segment.hpp"
#include "storage/block_manager.hpp"
#include "common/types/vector.hpp"

using namespace duckdb;
using namespace std;

ColumnSegment::ColumnSegment(BlockManager *manager, block_id_t block_id, index_t offset, index_t count, index_t start)
    : SegmentBase(start, count), block_id(block_id), offset(offset), manager(manager) {
}

ColumnSegment::ColumnSegment(index_t start)
    : SegmentBase(start, 0), block_id(INVALID_BLOCK), offset(0), manager(nullptr) {
}

data_ptr_t ColumnSegment::GetData() {
	if (!block) {
		lock_guard<mutex> lock(data_lock);
		if (!block) {
			block = make_unique<Block>(block_id);
			// no block yet: have to load the block
			if (block_id != INVALID_BLOCK) {
				// block is not an in-memory block: load the contents from disk
				// FIXME: this should be done using some buffer manager strategy
				manager->Read(*block);
			}
		}
	}
	return block->buffer;
}

data_ptr_t ColumnSegment::GetPointerToRow(TypeId type, index_t row) {
	assert(row >= start);
	// first check if the row is in this segment
	if (row >= start + count) {
		// not in this segment, check the next segment
		assert(next);
		auto &next_segment = (ColumnSegment &)*next;
		return next_segment.GetPointerToRow(type, row);
	}
	// row is in this segment, get the pointer
	index_t offset = row - start;
	return GetData() + offset * GetTypeIdSize(type);
}

void ColumnSegment::AppendValue(Vector &result, TypeId type, index_t row) {
	index_t type_size = GetTypeIdSize(type);
	memcpy(result.data + type_size * result.count, GetPointerToRow(type, row), type_size);
	result.count++;
}
