#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
FixedSizeAllocator::FixedSizeAllocator(const idx_t &allocation_size)
    : allocation_size(allocation_size), offsets_per_buffer(Storage::BLOCK_ALLOC_SIZE / allocation_size) {
}

FixedSizeAllocator::~FixedSizeAllocator() {
	for (auto &buffer : buffers) {
		Allocator::DefaultAllocator().FreeData(buffer, Storage::BLOCK_ALLOC_SIZE);
	}
}

idx_t FixedSizeAllocator::New() {

	// no more positions in the free list
	if (free_list.empty()) {

		// get a new buffer

		auto buffer = Allocator::DefaultAllocator().AllocateData(Storage::BLOCK_ALLOC_SIZE);
		idx_t buffer_id = buffers.size();
		D_ASSERT((buffer_id & BUFFER_ID_TO_ZERO) == 0);

		// add all new positions to the free list
		for (idx_t offset = 0; offset < offsets_per_buffer; offset++) {
			auto position = offset << sizeof(uint8_t) * 8 * 4;
			D_ASSERT((position & OFFSET_TO_ZERO) == 0);
			position |= buffer_id;
			D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
			free_list.push_back(position);
		}

		buffers.push_back(buffer);
	}

	// return the minimum position in the free list
	D_ASSERT(!free_list.empty());
	auto position = free_list.front();
	free_list.pop_front();
	D_ASSERT(free_list.size() <= buffers.size() * offsets_per_buffer);
	return position;
}

void FixedSizeAllocator::Free(const idx_t &position) {
	free_list.push_back(position);
	D_ASSERT(free_list.size() <= buffers.size() * offsets_per_buffer);
}

template <class T>
T *FixedSizeAllocator::Get(const idx_t &position) const {
	return (T *)Get(position);
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	if (offsets_per_buffer != other.offsets_per_buffer) {
		throw InternalException("Invalid FixedSizeAllocator for Combine.");
	}

	idx_t buffer_count = buffers.size();

	// merge the buffers
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
	other.buffers.clear();

	if (!buffer_count) {
		free_list = other.free_list;
		other.free_list.clear();
		D_ASSERT(free_list.size() <= buffers.size() * offsets_per_buffer);
		return;
	}

	// merge the free lists
	for (const auto &other_position : other.free_list) {
		auto position = other_position + buffer_count;
		D_ASSERT((other_position & BUFFER_ID_TO_ZERO) == (position & BUFFER_ID_TO_ZERO));
		free_list.push_back(position);
	}
	other.free_list.clear();
	D_ASSERT(free_list.size() <= buffers.size() * offsets_per_buffer);
}

bool FixedSizeAllocator::InitializeVacuum() {
	auto vacuum_count = free_list.size() / offsets_per_buffer / 2;
	vacuum_threshold = buffers.size() - vacuum_count;
	return vacuum_threshold < buffers.size();
}

void FixedSizeAllocator::FinalizeVacuum() {

	// free all (now unused) buffers
	while (vacuum_threshold < buffers.size()) {
		Allocator::DefaultAllocator().FreeData(buffers.back(), Storage::BLOCK_ALLOC_SIZE);
		buffers.pop_back();
	}

	// remove all invalid positions from the free list
	auto it = free_list.begin();
	while (it != free_list.end()) {
		if (NeedsVacuum(*it)) {
			it = free_list.erase(it);
		} else {
			it++;
		}
	}

	D_ASSERT(free_list.size() <= buffers.size() * offsets_per_buffer);
}

bool FixedSizeAllocator::NeedsVacuum(const idx_t &position) const {

	// get the buffer ID
	D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
	auto buffer_id = position & OFFSET_AND_FIRST_BYTE_TO_ZERO;

	if (buffer_id >= vacuum_threshold) {
		return true;
	}
	return false;
}

idx_t FixedSizeAllocator::Vacuum(const idx_t &position) {

	// don't push the free pointers back into the free list, because we will remove
	// all (then invalid) positions from the free list after the vacuum

	auto new_position = New();
	memcpy(Get(new_position), Get(position), allocation_size);
	return new_position;
}

data_ptr_t FixedSizeAllocator::Get(const idx_t &position) const {

	D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
	auto buffer_id = position & OFFSET_AND_FIRST_BYTE_TO_ZERO;
	auto offset = position >> sizeof(uint8_t) * 8 * 4;

	D_ASSERT(buffer_id < buffers.size());
	D_ASSERT(offset < offsets_per_buffer);
	return buffers[buffer_id] + offset * allocation_size;
}

} // namespace duckdb
