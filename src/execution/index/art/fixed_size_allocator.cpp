#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

namespace duckdb {
FixedSizeAllocator::FixedSizeAllocator(idx_t alloc_size) : alloc_size(alloc_size) {
	positions_per_buffer = Storage::BLOCK_ALLOC_SIZE / alloc_size;
}
FixedSizeAllocator::~FixedSizeAllocator() {
	for (auto &buffer : buffers) {
		Allocator().FreeData(buffer, Storage::BLOCK_ALLOC_SIZE);
	}
}

template <class T>
T *FixedSizeAllocator::GetDataAtPosition(idx_t position) {

	// TODO: first 4 bits of position hold the node type, remove/set 0

	auto bytes = position * alloc_size;
	auto buffer_idx = bytes % Storage::BLOCK_ALLOC_SIZE;
	D_ASSERT(buffer_idx < buffers.size());
	auto offset = bytes - buffer_idx * Storage::BLOCK_ALLOC_SIZE;
	D_ASSERT(offset < Storage::BLOCK_ALLOC_SIZE);
	return (T *)(buffers[buffer_idx] + offset);
}
idx_t FixedSizeAllocator::GetPosition() {

	// allocate a new buffer and add all available positions to the free_list
	if (free_list.empty()) {
		auto buffer = Allocator().AllocateData(Storage::BLOCK_ALLOC_SIZE);
		auto start = buffers.size() * positions_per_buffer;
		for (idx_t i = start; i < start + positions_per_buffer; i++) {
			// assert that leftmost bit is zero, i.e., the position is not mistaken for a swizzled pointer
			D_ASSERT(!((i >> (sizeof(i) * 8 - 1)) & 1));
			free_list.push_back(i);
		}
		buffers.push_back(buffer);
	}

	auto position = free_list.back();
	free_list.pop_back();
	return position;
}
void FixedSizeAllocator::FreePosition(const idx_t position) {
	free_list.push_back(position);
}

} // namespace duckdb
