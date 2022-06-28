#include "duckdb/storage/batched_allocator.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

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

BatchedAllocator::BatchedAllocator(Allocator &allocator, idx_t initial_capacity) : allocator(allocator) {
	head = nullptr;
	tail = nullptr;
	current_capacity = initial_capacity;
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

} // namespace duckdb
