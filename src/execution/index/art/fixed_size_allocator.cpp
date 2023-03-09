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
		free_list.emplace_back(0, list<idx_t>());

		auto buffer = Allocator::DefaultAllocator().AllocateData(Storage::BLOCK_ALLOC_SIZE);
		idx_t buffer_id = buffers.size();
		D_ASSERT((buffer_id & BUFFER_ID_TO_ZERO) == 0);

		// add all new positions to the free list
		for (idx_t offset = 0; offset < offsets_per_buffer; offset++) {
			auto position = offset << sizeof(uint8_t) * 8 * 4;
			D_ASSERT((position & OFFSET_TO_ZERO) == 0);
			position |= buffer_id;
			D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
			free_list.back().second.push_back(position);
		}

		buffers.push_back(buffer);
	}

	// return the minimum position in the free list
	D_ASSERT(!free_list.empty());
	D_ASSERT(!free_list.front().second.empty());
	auto buffer_id_offset = free_list.front().first;
	auto position = free_list.front().second.front();
	D_ASSERT((position & BUFFER_ID_TO_ZERO) == ((position + buffer_id_offset) & BUFFER_ID_TO_ZERO));

	// adjust the free list
	free_list.front().second.pop_front();
	if (free_list.front().second.empty()) {
		free_list.pop_front();
		// the first free list must always have a buffer ID offset of zero
		if (!free_list.empty() && free_list.front().first != 0) {
			RearrangeFreeList();
		}
	}

	// calculate and return the position
	position += buffer_id_offset;
	return position;
}

void FixedSizeAllocator::Free(const idx_t &position) {
	if (free_list.empty()) {
		free_list.emplace_back(0, list<idx_t>());
	}
	D_ASSERT(free_list.front().first == 0);
	free_list.front().second.push_back(position);
}

template <class T>
T *FixedSizeAllocator::Get(const idx_t &position) const {
	return (T *)Get(position);
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	if (offsets_per_buffer != other.offsets_per_buffer) {
		throw InternalException("Invalid FixedSizeAllocator for Combine.");
	}

	// remember the buffer ID offset and merge the buffers
	idx_t buffer_id_offset = buffers.size();
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
	other.buffers.clear();

	if (other.free_list.empty()) {
		return;
	}

	// increase the buffer offsets
	auto it = other.free_list.begin();
	for (; it != other.free_list.end(); it++) {
		it->first += buffer_id_offset;
	}
	// append the other free list to this free list
	free_list.splice(free_list.begin(), other.free_list);
	if (!free_list.empty() && free_list.front().first != 0) {
		RearrangeFreeList();
	}
}

bool FixedSizeAllocator::InitializeVacuum() {

	// get the vacuum threshold
	auto free_list_count = 0;
	for (const auto &list : free_list) {
		free_list_count += list.second.size();
	}
	auto vacuum_count = free_list_count / offsets_per_buffer / 2;
	vacuum_threshold = buffers.size() - vacuum_count;

	// remove all invalid positions from the free list to ensure that we do not reuse them
	if (vacuum_threshold < buffers.size()) {
		auto outer_it = free_list.begin();
		while (outer_it != free_list.end()) {
			auto buffer_id_offset = outer_it->first;
			auto inner_it = outer_it->second.begin();

			while (inner_it != outer_it->second.end()) {

				D_ASSERT((*inner_it & BUFFER_ID_TO_ZERO) == ((*inner_it + buffer_id_offset) & BUFFER_ID_TO_ZERO));
				if (NeedsVacuum(*inner_it + buffer_id_offset)) {
					inner_it = outer_it->second.erase(inner_it);
				} else {
					inner_it++;
				}
			}
		}
		return true;
	}

	return false;
}

void FixedSizeAllocator::FinalizeVacuum() {

	// free all (now unused) buffers
	while (vacuum_threshold < buffers.size()) {
		Allocator::DefaultAllocator().FreeData(buffers.back(), Storage::BLOCK_ALLOC_SIZE);
		buffers.pop_back();
	}
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

void FixedSizeAllocator::RearrangeFreeList() {

	auto buffer_id_offset = free_list.front().first;
	auto it = free_list.front().second.begin();
	for (; it != free_list.front().second.end(); it++) {
		D_ASSERT((*it & BUFFER_ID_TO_ZERO) == ((*it + buffer_id_offset) & BUFFER_ID_TO_ZERO));
		*it += buffer_id_offset;
	}
	free_list.front().first = 0;
}

} // namespace duckdb
