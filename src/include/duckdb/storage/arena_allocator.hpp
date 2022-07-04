//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/arena_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

struct ArenaChunk {
	ArenaChunk(Allocator &allocator, idx_t size);
	~ArenaChunk();

	unique_ptr<AllocatedData> data;
	idx_t current_position;
	idx_t maximum_size;
	unique_ptr<ArenaChunk> next;
	ArenaChunk *prev;
};

class ArenaAllocator {
	static constexpr const idx_t ARENA_ALLOCATOR_INITIAL_CAPACITY = 2048;

public:
	ArenaAllocator(Allocator &allocator, idx_t initial_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY);
	~ArenaAllocator();

	data_ptr_t Allocate(idx_t size);
	void Destroy();
	void Move(ArenaAllocator &allocator);

	ArenaChunk *GetHead();
	ArenaChunk *GetTail();

	bool IsEmpty();

private:
	//! Internal allocator that is used by the arena allocator
	Allocator &allocator;
	idx_t current_capacity;
	unique_ptr<ArenaChunk> head;
	ArenaChunk *tail;
};

} // namespace duckdb
