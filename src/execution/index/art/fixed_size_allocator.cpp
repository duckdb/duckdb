#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
FixedSizeAllocator::FixedSizeAllocator(const idx_t &allocation_size) : allocation_size(allocation_size) {

	// FIXME: magic heuristics
	if (allocation_size > 512) {
		// Node48 (664) and Node256 (2072)
		buffer_allocation_size = allocation_size * 8;

	} else if (allocation_size > 32) {
		// Node4 (64), LeafSegment (72), Node16 (168)
		buffer_allocation_size = allocation_size * 64;

	} else {
		// Leaf (32), PrefixSegment (72)
		buffer_allocation_size = allocation_size * 512;
	}

	offsets_per_buffer = buffer_allocation_size / allocation_size;
}

FixedSizeAllocator::~FixedSizeAllocator() {
	for (auto &buffer : buffers) {
		Allocator::DefaultAllocator().FreeData(buffer, buffer_allocation_size);
	}
}

FixedSizeAllocator::FixedSizeAllocator(FixedSizeAllocator &&other) noexcept {

	// public fields
	allocation_size = other.allocation_size;
	buffer_allocation_size = other.buffer_allocation_size;
	offsets_per_buffer = other.offsets_per_buffer;
	swap(buffers, other.buffers);
	swap(free_lists, other.free_lists);

	// private fields
	vacuum_threshold = other.vacuum_threshold;
}

FixedSizeAllocator &FixedSizeAllocator::operator=(FixedSizeAllocator &&other) noexcept {

	// public fields
	allocation_size = other.allocation_size;
	buffer_allocation_size = other.buffer_allocation_size;
	offsets_per_buffer = other.offsets_per_buffer;
	swap(buffers, other.buffers);
	swap(free_lists, other.free_lists);

	// private fields
	vacuum_threshold = other.vacuum_threshold;

	return *this;
}

void FixedSizeAllocator::New(idx_t &new_position) {

	// no more positions in the free list
	if (free_lists.empty()) {

		// add a new free list
		free_lists.emplace_back(0);
		idx_t buffer_id = buffers.size();
		D_ASSERT((buffer_id & BUFFER_ID_TO_ZERO) == 0);

		// add all new positions to the free list
		for (idx_t offset = 0; offset < offsets_per_buffer; offset++) {
			auto position = offset << OFFSET_SHIFT;
			D_ASSERT((position & OFFSET_TO_ZERO) == 0);
			position |= buffer_id;
			D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
			free_lists.back().list.push_back(position);
		}

		// add a new buffer
		auto buffer = Allocator::DefaultAllocator().AllocateData(buffer_allocation_size);
		buffers.push_back(buffer);
	}

	// return a free position
	D_ASSERT(!free_lists.empty());
	D_ASSERT(!free_lists.front().list.empty());
	auto buffer_id_offset = free_lists.front().buffer_id_offset;
	new_position = free_lists.front().list.front();
	D_ASSERT((new_position & BUFFER_ID_TO_ZERO) == ((new_position + buffer_id_offset) & BUFFER_ID_TO_ZERO));

	// adjust the free lists
	free_lists.front().list.pop_front();
	if (free_lists.front().list.empty()) {
		free_lists.pop_front();
		RearrangeFreeList();
	}

	// calculate the position
	new_position += buffer_id_offset;
}

void FixedSizeAllocator::Free(const idx_t &position) {
	if (free_lists.empty()) {
		free_lists.emplace_back(0);
	}
	D_ASSERT(free_lists.front().buffer_id_offset == 0);
	free_lists.front().list.push_back(position);
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	if (allocation_size != other.allocation_size) {
		throw InternalException("Invalid FixedSizeAllocator for Merge.");
	}

	// remember the buffer ID offset and merge the buffers
	idx_t buffer_id_offset = buffers.size();
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
	other.buffers.clear();

	if (other.free_lists.empty()) {
		return;
	}

	// increase the buffer offsets
	auto free_lists_it = other.free_lists.begin();
	for (; free_lists_it != other.free_lists.end(); free_lists_it++) {
		free_lists_it->buffer_id_offset += buffer_id_offset;
	}

	// append the other free list to this free list
	free_lists.splice(free_lists.begin(), other.free_lists);
	RearrangeFreeList();
}

bool FixedSizeAllocator::InitializeVacuum() {

	// get the vacuum threshold
	auto free_list_count = 0;
	for (const auto &free_list : free_lists) {
		free_list_count += free_list.list.size();
	}

	auto vacuum_count = free_list_count / offsets_per_buffer / 2;
	vacuum_threshold = buffers.size() - vacuum_count;

	// remove all invalid positions from the free list to ensure that we do not reuse them
	if (vacuum_threshold < buffers.size()) {

		auto free_lists_it = free_lists.begin();
		while (free_lists_it != free_lists.end()) {

			auto buffer_id_offset = free_lists_it->buffer_id_offset;
			auto list_it = free_lists_it->list.begin();

			while (list_it != free_lists_it->list.end()) {

				D_ASSERT((*list_it & BUFFER_ID_TO_ZERO) == ((*list_it + buffer_id_offset) & BUFFER_ID_TO_ZERO));
				if (NeedsVacuum(*list_it + buffer_id_offset)) {
					list_it = free_lists_it->list.erase(list_it);
				} else {
					list_it++;
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
		Allocator::DefaultAllocator().FreeData(buffers.back(), buffer_allocation_size);
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

void FixedSizeAllocator::Vacuum(idx_t &position) {

	// don't push the free pointers back into the free list, because we will remove
	// all (then invalid) positions from the free list after the vacuum

	auto new_position = New();
	memcpy(Get(new_position), Get(position), allocation_size);
	position = new_position;
}

void FixedSizeAllocator::RearrangeFreeList() {

	// the first free list must always have a buffer ID offset of zero
	if (!free_lists.empty() && free_lists.front().buffer_id_offset != 0) {

		auto buffer_id_offset = free_lists.front().buffer_id_offset;
		auto list_it = free_lists.front().list.begin();
		for (; list_it != free_lists.front().list.end(); list_it++) {
			D_ASSERT((*list_it & BUFFER_ID_TO_ZERO) == ((*list_it + buffer_id_offset) & BUFFER_ID_TO_ZERO));
			*list_it += buffer_id_offset;
		}
		free_lists.front().buffer_id_offset = 0;
	}
}

} // namespace duckdb
