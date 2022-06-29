//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/batched_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
struct ArenaAllocatorDebugInfo;

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
	static constexpr const idx_t ARENA_ALLOCATOR_INITIAL_CAPACITY = 4096;

public:
	ArenaAllocator(Allocator &allocator, idx_t initial_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY);
	~ArenaAllocator();

	data_ptr_t Allocate(idx_t size);

	ArenaChunk *GetHead();
	ArenaChunk *GetTail();

	bool IsEmpty();

	static data_ptr_t ArenaAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	static void ArenaAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t ArenaAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);

	Allocator &GetArenaAllocator();

	ArenaAllocatorDebugInfo &GetDebugInfo();

private:
	//! Internal allocator that is used by the arena allocator
	Allocator &allocator;
	idx_t current_capacity;
	unique_ptr<ArenaChunk> head;
	ArenaChunk *tail;
	//! Allocator associated with the arena allocator, that passes all allocations through it
	Allocator batched_allocator;
	//! Debug info - only used in debug mode
	unique_ptr<ArenaAllocatorDebugInfo> debug_info;
};

} // namespace duckdb
