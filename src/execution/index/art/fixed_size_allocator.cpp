#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

namespace duckdb {
FixedSizeAllocator::FixedSizeAllocator(const idx_t &allocation_size)
    : allocation_size(allocation_size), offsets_per_buffer(Storage::BLOCK_ALLOC_SIZE / allocation_size) {
}

FixedSizeAllocator::~FixedSizeAllocator() {
	for (auto &buffer : buffers) {
		Allocator().FreeData(buffer, Storage::BLOCK_ALLOC_SIZE);
	}
}

idx_t FixedSizeAllocator::New() {

	// no more positions in the free list
	if (free_list.empty()) {

		// get a new buffer
		auto buffer = Allocator().AllocateData(Storage::BLOCK_ALLOC_SIZE);
		idx_t buffer_id = buffers.size();
		D_ASSERT((buffer_id & 0xffff0000) == 0);

		// add all new positions to the free list
		for (idx_t offset = 0; offset < offsets_per_buffer; offset++) {
			auto position = offset << sizeof(uint8_t) * 8 * 4;
			D_ASSERT((position & 0xf000ffff) == 0);
			position |= buffer_id;
			D_ASSERT((position & 0xf0000000) == 0);
			free_list.push(position);
		}

		buffers.push_back(buffer);
	}

	// return the minimum position in the free list
	auto position = free_list.top();
	free_list.pop();
	return position;
}

void FixedSizeAllocator::Free(const idx_t &position) {
	free_list.push(position);
}

template <class T>
T *FixedSizeAllocator::Get(const idx_t &position) const {
	return (T *)Get(position);
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	if (offsets_per_buffer != other.offsets_per_buffer) {
		throw InternalException("Invalid FixedSizeAllocator for Combine.");
	}

	// merge the free lists
	idx_t buffer_count = buffers.size();
	while (!other.free_list.empty()) {
		auto position = other.free_list.top() + buffer_count;
		D_ASSERT((other.free_list.top() & 0xffff0000) == (position & 0xffff0000));
		free_list.push(position);
		other.free_list.pop();
	}

	// merge the buffers
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
}

idx_t FixedSizeAllocator::VacuumCount() {
	return free_list.size() / offsets_per_buffer / 2;
}

idx_t FixedSizeAllocator::Vacuum(const idx_t &position) {

	if (!VacuumCount()) {
		return position;
	}

	// TODO: ideally, check the vacuum count somewhere else
	// TODO: ideally, also check if the position qualifies for a vacuum somewhere else
	// TODO: don't push the free pointers back into the free list

	auto new_position = New();
	memcpy(Get(new_position), Get(position), allocation_size);
	return new_position;
}

data_ptr_t FixedSizeAllocator::Get(const idx_t &position) const {

	D_ASSERT((position & 0xf0000000) == 0);
	auto buffer_id = position & 0x0000ffff;
	auto offset = position >> sizeof(uint8_t) * 8 * 4;

	D_ASSERT(buffer_id < buffers.size());
	D_ASSERT(offset < offsets_per_buffer);
	return buffers[buffer_id] + offset;
}

} // namespace duckdb
