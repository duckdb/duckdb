#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
FixedSizeAllocator::FixedSizeAllocator(const idx_t &allocation_size)
    : allocation_size(allocation_size), total_allocations(0) {

	idx_t bits_per_value = sizeof(validity_t) * 8;
	idx_t curr_alloc_size = 0;

	bitmask_count = 0;
	allocations_per_buffer = 0;

	while (curr_alloc_size < BUFFER_ALLOCATION_SIZE) {
		if (!bitmask_count || (bitmask_count * bits_per_value) % allocations_per_buffer == 0) {
			bitmask_count++;
			curr_alloc_size += sizeof(validity_t);
		}

		auto remaining_alloc_size = BUFFER_ALLOCATION_SIZE - curr_alloc_size;
		auto remaining_allocations = MinValue(remaining_alloc_size / allocation_size, bits_per_value);

		if (remaining_allocations == 0) {
			break;
		}

		allocations_per_buffer += remaining_allocations;
		curr_alloc_size += remaining_allocations * allocation_size;
	}

	allocation_offset = bitmask_count * sizeof(validity_t);
}

FixedSizeAllocator::~FixedSizeAllocator() {
	for (auto &buffer : buffers) {
		Allocator::DefaultAllocator().FreeData(buffer.ptr, BUFFER_ALLOCATION_SIZE);
	}
}

void FixedSizeAllocator::New(idx_t &new_position) {

	// no more positions in the free list
	if (buffers_with_free_space.empty()) {

		// add the new buffer
		idx_t buffer_id = buffers.size();
		D_ASSERT((buffer_id & BUFFER_ID_TO_ZERO) == 0);
		auto buffer = Allocator::DefaultAllocator().AllocateData(BUFFER_ALLOCATION_SIZE);
		buffers.emplace_back(buffer, 0);
		buffers_with_free_space.insert(buffer_id);

		// set the bitmask
		ValidityMask mask((validity_t *)buffer);
		mask.SetAllValid(allocations_per_buffer);
	}

	// return a free position
	D_ASSERT(!buffers_with_free_space.empty());
	auto buffer_id = *buffers_with_free_space.begin();

	auto ptr = (validity_t *)buffers[buffer_id].ptr;
	ValidityMask mask(ptr);
	new_position = buffer_id + GetOffset(mask, buffers[buffer_id].allocation_count);

	buffers[buffer_id].allocation_count++;
	total_allocations++;
	if (buffers[buffer_id].allocation_count == allocations_per_buffer) {
		buffers_with_free_space.erase(buffer_id);
	}
}

void FixedSizeAllocator::Free(const idx_t &position) {

	auto buffer_id = position & OFFSET_AND_FIRST_BYTE_TO_ZERO;
	auto offset = (position & BUFFER_ID_TO_ZERO) >> OFFSET_SHIFT;

	auto ptr = (validity_t *)buffers[buffer_id].ptr;
	ValidityMask mask(ptr);
	D_ASSERT(!mask.RowIsValid(offset));
	mask.SetValid(offset);
	buffers_with_free_space.insert(buffer_id);

	D_ASSERT(total_allocations > 0);
	D_ASSERT(buffers[buffer_id].allocation_count > 0);
	buffers[buffer_id].allocation_count--;
	total_allocations--;
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	if (allocation_size != other.allocation_size) {
		throw InternalException("Invalid FixedSizeAllocator for Merge.");
	}

	// remember the buffer count and merge the buffers
	idx_t buffer_count = buffers.size();
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
	other.buffers.clear();

	// merge the vectors containing all buffers with free space
	for (auto &buffer_id : other.buffers_with_free_space) {
		buffers_with_free_space.insert(buffer_id + buffer_count);
	}
	other.buffers_with_free_space.clear();

	// add the total allocations
	total_allocations += other.total_allocations;
}

bool FixedSizeAllocator::InitializeVacuum() {

	auto total_available_allocations = allocations_per_buffer * buffers.size();
	auto total_free_positions = total_available_allocations - total_allocations;

	// vacuum_count buffers can be freed
	auto vacuum_count = total_free_positions / allocations_per_buffer / 2;
	if (vacuum_count < VACUUM_THRESHOLD) {
		return false;
	}
	vacuum_threshold = buffers.size() - vacuum_count;

	// remove all invalid buffers from the available buffer list to ensure that we do not reuse them
	auto it = buffers_with_free_space.begin();
	while (it != buffers_with_free_space.end()) {
		if (*it >= vacuum_threshold) {
			it = buffers_with_free_space.erase(it);
		} else {
			it++;
		}
	}

	return true;
}

void FixedSizeAllocator::FinalizeVacuum() {

	// free all (now unused) buffers
	while (vacuum_threshold < buffers.size()) {
		Allocator::DefaultAllocator().FreeData(buffers.back().ptr, BUFFER_ALLOCATION_SIZE);
		buffers.pop_back();
	}
}

idx_t FixedSizeAllocator::Vacuum(const idx_t &position) {

	// we do not need to adjust the bitmask of the old buffer, because we will free the entire
	// buffer after vacuum

	auto new_position = New();
	memcpy(Get(new_position), Get(position), allocation_size);
	return new_position;
}

idx_t FixedSizeAllocator::GetOffset(ValidityMask &mask, const idx_t &allocation_count) {

	auto data = mask.GetData();

	// fills up a buffer sequentially before searching for free bits
	if (mask.RowIsValid(allocation_count)) {
		mask.SetInvalid(allocation_count);
		return allocation_count << OFFSET_SHIFT;
	}

	// get an entry with free bits
	for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
		if (data[entry_idx] != 0) {

			// find the position of the free bit
			auto entry = data[entry_idx];
			idx_t first_valid_bit = 0;

			for (idx_t i = 0; i < 6; i++) {
				if (entry & BASE[i]) {
					// first valid bit is in the rightmost s[i] bits
					entry &= BASE[i];
				} else {
					entry >>= SHIFT[i];
					first_valid_bit += SHIFT[i];
				}
			}
			D_ASSERT(entry);

			auto prev_bits = entry_idx * sizeof(validity_t) * 8;
			D_ASSERT(mask.RowIsValid(prev_bits + first_valid_bit));
			mask.SetInvalid(prev_bits + first_valid_bit);
			return (prev_bits + first_valid_bit) << OFFSET_SHIFT;
		}
	}

	throw InternalException("Invalid bitmask of FixedSizeAllocator");
}

} // namespace duckdb
