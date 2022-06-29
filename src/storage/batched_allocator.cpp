#include "duckdb/storage/batched_allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#ifdef DUCKDB_DEBUG_ALLOCATION
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include <execinfo.h>
#endif

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

struct BatchedAllocatorDebugInfo {
	~BatchedAllocatorDebugInfo() {
#ifdef DUCKDB_DEBUG_ALLOCATION
		if (allocation_count != 0) {
			printf("Outstanding allocations found for BatchedAllocator\n");
			for (auto &entry : pointers) {
				printf("Allocation of size %lld at address %p\n", entry.second.first, (void *)entry.first);
				printf("Stack trace:\n%s\n", entry.second.second.c_str());
				printf("\n");
			}
		}
#endif
		//! Verify that there is no outstanding memory still associated with the batched allocator
		//! Only works for access to the batched allocator through the batched allocator interface
		//! If this assertion triggers, enable DUCKDB_DEBUG_ALLOCATION for more information about the allocations
		D_ASSERT(allocation_count == 0);
	}

	//! The number of bytes that are outstanding (i.e. that have been allocated - but not freed)
	//! Used for debug purposes
	idx_t allocation_count = 0;
#ifdef DUCKDB_DEBUG_ALLOCATION
	//! Set of active outstanding pointers together with stack traces
	unordered_map<data_ptr_t, pair<idx_t, string>> pointers;
#endif
};

BatchedAllocator::BatchedAllocator(Allocator &allocator, idx_t initial_capacity)
    : allocator(allocator), batched_allocator(BatchedAllocatorAllocate, BatchedAllocatorFree, BatchedAllocatorRealloc,
                                              make_unique<BatchedAllocatorData>(*this)) {
	head = nullptr;
	tail = nullptr;
	current_capacity = initial_capacity;
#ifdef DEBUG
	debug_info = make_unique<BatchedAllocatorDebugInfo>();
#endif
}

BatchedAllocator::~BatchedAllocator() {
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

BatchedAllocatorDebugInfo &BatchedAllocator::GetDebugInfo() {
#ifndef DEBUG
	throw InternalException("Debug info can only be used in debug mode");
#else
	D_ASSERT(debug_info);
	return *debug_info;
#endif
}

//===--------------------------------------------------------------------===//
// Allocator
//===--------------------------------------------------------------------===//
#ifdef DUCKDB_DEBUG_ALLOCATION
inline string GetStackTrace(int max_depth = 128) {
	string result;
	auto callstack = unique_ptr<void *[]>(new void *[max_depth]);
	int frames = backtrace(callstack.get(), max_depth);
	char **strs = backtrace_symbols(callstack.get(), frames);
	for (int i = 0; i < frames; i++) {
		result += strs[i];
		result += "\n";
	}
	free(strs);
	return result;
}
#endif

data_ptr_t BatchedAllocator::BatchedAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = (BatchedAllocatorData &)*private_data;
	auto result = data.batched_allocator.Allocate(size);
#ifdef DEBUG
	auto &debug_info = data.batched_allocator.GetDebugInfo();
	debug_info.allocation_count += size;
#ifdef DUCKDB_DEBUG_ALLOCATION
	debug_info.pointers[result] = make_pair(size, GetStackTrace());
#endif
#endif
	return result;
}

void BatchedAllocator::BatchedAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
#ifdef DEBUG
	auto &data = (BatchedAllocatorData &)*private_data;
	auto &debug_info = data.batched_allocator.GetDebugInfo();
	D_ASSERT(debug_info.allocation_count >= size);
	debug_info.allocation_count -= size;
#ifdef DUCKDB_DEBUG_ALLOCATION
	D_ASSERT(debug_info.pointers.find(pointer) != debug_info.pointers.end());
	debug_info.pointers.erase(pointer);
#endif
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
