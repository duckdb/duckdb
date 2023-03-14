#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
FixedSizeAllocator::FixedSizeAllocator(const idx_t &allocation_size)
    : allocation_size(allocation_size), total_allocations(0) {

	// requires one bitmask value (validity_t, 8 bytes) for 64 allocations
	// (allocation_size * 64 + 8) * x <= BUFFER_ALLOCATION_SIZE
	bitmask_count = BUFFER_ALLOCATION_SIZE / (allocation_size * 64 + 8);
	allocation_offset = bitmask_count * 8;
	allocations_per_buffer = bitmask_count * 64;
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
	new_position = buffer_id + GetOffset(mask);

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

idx_t FixedSizeAllocator::GetOffset(ValidityMask &mask) {

	auto data = mask.GetData();

	// get an entry with free bits
	for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
		if (data[entry_idx] != 0) {

			// bit-shifting binary search-ish thingy
			if ((data[entry_idx] >> OFFSET_SHIFT) == 0) {
				// valid in the
			}

			// get the position of the first free bit
			auto prev_bits = entry_idx * sizeof(validity_t) * 8;
			for (idx_t i = 0; i < sizeof(validity_t) * 8; i++) {
				if (mask.RowIsValid(prev_bits + i)) {
					mask.SetInvalid(prev_bits + i);
					return (prev_bits + i) << OFFSET_SHIFT;
				}
			}
		}
	}

	throw InternalException("Invalid bitmask of FixedSizeAllocator");
}

} // namespace duckdb
