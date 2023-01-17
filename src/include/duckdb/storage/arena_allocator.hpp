//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/arena_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

struct ArenaChunk {
	ArenaChunk(Allocator &allocator, idx_t size);
	~ArenaChunk();

	AllocatedData data;
	idx_t current_position;
	idx_t maximum_size;
	unique_ptr<ArenaChunk> next;
	ArenaChunk *prev;
};

class ArenaAllocator {
	static constexpr const idx_t ARENA_ALLOCATOR_INITIAL_CAPACITY = 2048;

public:
	DUCKDB_API ArenaAllocator(Allocator &allocator, idx_t initial_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY);
	DUCKDB_API ~ArenaAllocator();

	DUCKDB_API data_ptr_t Allocate(idx_t size);
	DUCKDB_API data_ptr_t Reallocate(data_ptr_t pointer, idx_t old_size, idx_t size);

	DUCKDB_API data_ptr_t AllocateAligned(idx_t size);
	DUCKDB_API data_ptr_t ReallocateAligned(data_ptr_t pointer, idx_t old_size, idx_t size);

	//! Resets the current head and destroys all previous arena chunks
	DUCKDB_API void Reset();
	DUCKDB_API void Destroy();
	DUCKDB_API void Move(ArenaAllocator &allocator);

	DUCKDB_API ArenaChunk *GetHead();
	DUCKDB_API ArenaChunk *GetTail();

	DUCKDB_API bool IsEmpty();

private:
	//! Internal allocator that is used by the arena allocator
	Allocator &allocator;
	idx_t current_capacity;
	unique_ptr<ArenaChunk> head;
	ArenaChunk *tail;
};

} // namespace duckdb
