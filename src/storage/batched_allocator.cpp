#include "duckdb/storage/batched_allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct BatchedAllocatorData : PrivateAllocatorData {
	explicit BatchedAllocatorData(BatchedAllocator &batched_allocator) : batched_allocator(batched_allocator) {
	}

	BatchedAllocator &batched_allocator;
	idx_t allocation_counter = 0;
};

AllocatedChunk::AllocatedChunk(Allocator &allocator, idx_t size)
    : current_position(0), maximum_size(size), prev(nullptr) {
	D_ASSERT(size > 0);
	data = allocator.Allocate(size);
}
AllocatedChunk::~AllocatedChunk() {
	if (next) {
		auto current_next = move(next);
		while (current_next) {
			current_next = move(current_next->next);
		}
	}
}

BatchedAllocator::BatchedAllocator(Allocator &allocator, idx_t initial_capacity)
    : allocator(allocator), batched_allocator(BatchedAllocatorAllocate, BatchedAllocatorFree, BatchedAllocatorRealloc,
                                              make_unique<BatchedAllocatorData>(*this)),
      allocation_count(0) {
	head = nullptr;
	tail = nullptr;
	current_capacity = initial_capacity;
}

BatchedAllocator::~BatchedAllocator() {
	//! Verify that there is no outstanding memory still associated with the batched allocator
	//! Only works for access to the batched allocator through the batched allocator interface
	D_ASSERT(allocation_count == 0);
}

data_ptr_t BatchedAllocator::Allocate(idx_t len) {
	D_ASSERT(!head || head->current_position <= head->maximum_size);
	if (!head || head->current_position + len > head->maximum_size) {
		do {
			current_capacity *= 2;
		} while (current_capacity < len);
		auto new_chunk = make_unique<AllocatedChunk>(allocator, current_capacity);
		if (head) {
			head->prev = new_chunk.get();
			new_chunk->next = move(head);
		} else {
			tail = new_chunk.get();
		}
		head = move(new_chunk);
	}
	D_ASSERT(head->current_position + len <= head->maximum_size);
	auto result = head->data->get() + head->current_position;
	head->current_position += len;
	return result;
}

AllocatedChunk *BatchedAllocator::GetHead() {
	return head.get();
}

AllocatedChunk *BatchedAllocator::GetTail() {
	return tail;
}

bool BatchedAllocator::IsEmpty() {
	return head == nullptr;
}

void BatchedAllocator::AddAllocation(idx_t size) {
	allocation_count += size;
}

void BatchedAllocator::FreeAllocation(idx_t size) {
	D_ASSERT(allocation_count >= size);
	allocation_count -= size;
}

//===--------------------------------------------------------------------===//
// Allocator
//===--------------------------------------------------------------------===//
data_ptr_t BatchedAllocator::BatchedAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (BatchedAllocatorData &)*private_data;
#ifdef DEBUG
	data.batched_allocator.AddAllocation(size);
#endif
	return data.batched_allocator.Allocate(size);
}

void BatchedAllocator::BatchedAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
#ifdef DEBUG
	auto &data = (BatchedAllocatorData &)*private_data;
	data.batched_allocator.FreeAllocation(size);
#endif
}

data_ptr_t BatchedAllocator::BatchedAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer,
                                                     idx_t size) {
	throw InternalException("FIXME: realloc not implemented for batched allocator");
}

Allocator &BatchedAllocator::GetBatchedAllocator() {
	return batched_allocator;
}

} // namespace duckdb
