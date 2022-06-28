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

struct AllocatedChunk {
	AllocatedChunk(Allocator &allocator, idx_t size);
	~AllocatedChunk();

	unique_ptr<AllocatedData> data;
	idx_t current_position;
	idx_t maximum_size;
	unique_ptr<AllocatedChunk> next;
	AllocatedChunk *prev;
};

class BatchedAllocator {
	static constexpr const idx_t BATCHED_ALLOCATOR_INITIAL_CAPACITY = 4096;

public:
	BatchedAllocator(Allocator &allocator, idx_t initial_capacity = BATCHED_ALLOCATOR_INITIAL_CAPACITY);

	data_ptr_t Allocate(idx_t size);

	AllocatedChunk *GetHead();
	AllocatedChunk *GetTail();

	bool IsEmpty();

private:
	Allocator &allocator;
	idx_t current_capacity;
	unique_ptr<AllocatedChunk> head;
	AllocatedChunk *tail;
};

} // namespace duckdb
