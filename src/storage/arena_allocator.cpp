#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

ArenaChunk::ArenaChunk(Allocator &allocator, idx_t size) : current_position(0), maximum_size(size), prev(nullptr) {
	D_ASSERT(size > 0);
	data = allocator.Allocate(size);
}
ArenaChunk::~ArenaChunk() {
	if (next) {
		auto current_next = Move(next);
		while (current_next) {
			current_next = Move(current_next->next);
		}
	}
}

ArenaAllocator::ArenaAllocator(Allocator &allocator, idx_t initial_capacity) : allocator(allocator) {
	head = nullptr;
	tail = nullptr;
	current_capacity = initial_capacity;
}

ArenaAllocator::~ArenaAllocator() {
}

data_ptr_t ArenaAllocator::Allocate(idx_t len) {
	D_ASSERT(!head || head->current_position <= head->maximum_size);
	if (!head || head->current_position + len > head->maximum_size) {
		do {
			current_capacity *= 2;
		} while (current_capacity < len);
		auto new_chunk = make_unique<ArenaChunk>(allocator, current_capacity);
		if (head) {
			head->prev = new_chunk.get();
			new_chunk->next = Move(head);
		} else {
			tail = new_chunk.get();
		}
		head = Move(new_chunk);
	}
	D_ASSERT(head->current_position + len <= head->maximum_size);
	auto result = head->data.get() + head->current_position;
	head->current_position += len;
	return result;
}

void ArenaAllocator::Reset() {

	if (head) {
		// destroy all chunks except the current one
		if (head->next) {
			auto current_next = Move(head->next);
			while (current_next) {
				current_next = Move(current_next->next);
			}
		}
		tail = head.get();

		// reset the head
		head->current_position = 0;
		head->prev = nullptr;
	}
}

void ArenaAllocator::Destroy() {
	head = nullptr;
	tail = nullptr;
	current_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY;
}

void ArenaAllocator::MoveAllocator(ArenaAllocator &allocator) {
	D_ASSERT(!allocator.head);
	allocator.tail = tail;
	allocator.head = Move(head);
	allocator.current_capacity = current_capacity;
	Destroy();
}

ArenaChunk *ArenaAllocator::GetHead() {
	return head.get();
}

ArenaChunk *ArenaAllocator::GetTail() {
	return tail;
}

bool ArenaAllocator::IsEmpty() {
	return head == nullptr;
}

} // namespace duckdb
