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
struct BatchedAllocatorDebugInfo;

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
	~BatchedAllocator();

	data_ptr_t Allocate(idx_t size);

	AllocatedChunk *GetHead();
	AllocatedChunk *GetTail();

	bool IsEmpty();

	static data_ptr_t BatchedAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size);
	static void BatchedAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
	static data_ptr_t BatchedAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);

	Allocator &GetBatchedAllocator();

	BatchedAllocatorDebugInfo &GetDebugInfo();

private:
	//! Internal allocator that is used by the batched allocator
	Allocator &allocator;
	idx_t current_capacity;
	unique_ptr<AllocatedChunk> head;
	AllocatedChunk *tail;
	//! Allocator associated with the batched allocator, that passes all allocations through it
	Allocator batched_allocator;
	//! Debug info - only used in debug mode
	unique_ptr<BatchedAllocatorDebugInfo> debug_info;
};

} // namespace duckdb
