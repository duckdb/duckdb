#include "storage/table/persistent_segment.hpp"
#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

PersistentSegment::PersistentSegment(BlockManager &manager, block_id_t id, TypeId type, index_t start, index_t count) :
	ColumnSegment(type, ColumnSegmentType::PERSISTENT, start, count), manager(manager), block_id(id) {
}

void PersistentSegment::LoadBlock() {
	if (block) {
		// already loaded
		return;
	}
	// load the block
	lock_guard<mutex> lock(load_lock);
	if (block) {
		// loaded in the meantime
		return;
	}
	block = make_unique<Block>(block_id);
	manager.Read(*block);
}

void PersistentSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count) {
	LoadBlock();

	data_ptr_t dataptr = block->buffer + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = count;
	VectorOperations::AppendFromStorage(source, result, stats.has_null);
	pointer.offset += count;
}

void PersistentSegment::Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) {
	LoadBlock();

	data_ptr_t dataptr = block->buffer + pointer.offset * type_size;
	Vector source(type, dataptr);
	source.count = sel_count;
	source.sel_vector = sel_vector;
	VectorOperations::AppendFromStorage(source, result, stats.has_null);
	pointer.offset += count;
}

void PersistentSegment::Fetch(Vector &result, index_t row_id) {
	LoadBlock();

	assert(row_id >= start);
	if (row_id >= start + count) {
		assert(next);
		auto &next_segment = (ColumnSegment&) *next;
		next_segment.Fetch(result, row_id);
		return;
	}
	data_ptr_t dataptr = block->buffer + (row_id - start) * type_size;
	Vector source(type, dataptr);
	source.count = 1;
	VectorOperations::AppendFromStorage(source, result, stats.has_null);
}
