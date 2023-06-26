#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

namespace duckdb {

constexpr idx_t FixedSizeAllocator::BASE[];
constexpr uint8_t FixedSizeAllocator::SHIFT[];

FixedSizeAllocator::FixedSizeAllocator(const idx_t allocation_size, Allocator &allocator)
    : allocation_size(allocation_size), total_allocations(0), allocator(allocator) {

	// calculate how many allocations fit into one buffer

	idx_t bits_per_value = sizeof(validity_t) * 8;
	idx_t curr_alloc_size = 0;

	bitmask_count = 0;
	allocations_per_buffer = 0;

	while (curr_alloc_size < BUFFER_ALLOC_SIZE) {
		if (!bitmask_count || (bitmask_count * bits_per_value) % allocations_per_buffer == 0) {
			bitmask_count++;
			curr_alloc_size += sizeof(validity_t);
		}

		auto remaining_alloc_size = BUFFER_ALLOC_SIZE - curr_alloc_size;
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
		allocator.FreeData(buffer.ptr, BUFFER_ALLOC_SIZE);
	}
}

Node FixedSizeAllocator::New() {

	// no more free pointers
	if (buffers_with_free_space.empty()) {

		// add a new buffer
		idx_t buffer_id = buffers.size();
		D_ASSERT(buffer_id <= (uint32_t)DConstants::INVALID_INDEX);
		auto buffer = allocator.AllocateData(BUFFER_ALLOC_SIZE);
		buffers.emplace_back(buffer, 0);
		buffers_with_free_space.insert(buffer_id);

		// set the bitmask
		ValidityMask mask(reinterpret_cast<validity_t *>(buffer));
		mask.SetAllValid(allocations_per_buffer);
	}

	// return a pointer
	D_ASSERT(!buffers_with_free_space.empty());
	auto buffer_id = (uint32_t)*buffers_with_free_space.begin();

	auto bitmask_ptr = reinterpret_cast<validity_t *>(buffers[buffer_id].ptr);
	ValidityMask mask(bitmask_ptr);
	auto offset = GetOffset(mask, buffers[buffer_id].allocation_count);

	buffers[buffer_id].allocation_count++;
	total_allocations++;
	if (buffers[buffer_id].allocation_count == allocations_per_buffer) {
		buffers_with_free_space.erase(buffer_id);
	}

	return Node(buffer_id, offset);
}

void FixedSizeAllocator::Free(const Node ptr) {
	auto bitmask_ptr = reinterpret_cast<validity_t *>(buffers[ptr.GetBufferId()].ptr);
	ValidityMask mask(bitmask_ptr);
	D_ASSERT(!mask.RowIsValid(ptr.GetOffset()));
	mask.SetValid(ptr.GetOffset());
	buffers_with_free_space.insert(ptr.GetBufferId());

	D_ASSERT(total_allocations > 0);
	D_ASSERT(buffers[ptr.GetBufferId()].allocation_count > 0);
	buffers[ptr.GetBufferId()].allocation_count--;
	total_allocations--;
}

void FixedSizeAllocator::Reset() {

	for (auto &buffer : buffers) {
		allocator.FreeData(buffer.ptr, BUFFER_ALLOC_SIZE);
	}
	buffers.clear();
	buffers_with_free_space.clear();
	total_allocations = 0;
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	D_ASSERT(allocation_size == other.allocation_size);

	// remember the buffer count and merge the buffers
	idx_t buffer_count = buffers.size();
	for (auto &buffer : other.buffers) {
		buffers.push_back(buffer);
	}
	other.buffers.clear();

	// merge the buffers with free spaces
	for (auto &buffer_id : other.buffers_with_free_space) {
		buffers_with_free_space.insert(buffer_id + buffer_count);
	}
	other.buffers_with_free_space.clear();

	// add the total allocations
	total_allocations += other.total_allocations;
}

bool FixedSizeAllocator::InitializeVacuum() {

	if (total_allocations == 0) {
		Reset();
		return false;
	}

	auto total_available_allocations = allocations_per_buffer * buffers.size();
	D_ASSERT(total_available_allocations >= total_allocations);
	auto total_free_positions = total_available_allocations - total_allocations;

	// vacuum_count buffers can be freed
	auto vacuum_count = total_free_positions / allocations_per_buffer;

	// calculate the vacuum threshold adaptively
	D_ASSERT(vacuum_count < buffers.size());
	idx_t memory_usage = GetMemoryUsage();
	idx_t excess_memory_usage = vacuum_count * BUFFER_ALLOC_SIZE;
	auto excess_percentage = (double)excess_memory_usage / (double)memory_usage;
	auto threshold = (double)VACUUM_THRESHOLD / 100.0;
	if (excess_percentage < threshold) {
		return false;
	}

	min_vacuum_buffer_id = buffers.size() - vacuum_count;

	// remove all invalid buffers from the available buffer list to ensure that we do not reuse them
	auto it = buffers_with_free_space.begin();
	while (it != buffers_with_free_space.end()) {
		if (*it >= min_vacuum_buffer_id) {
			it = buffers_with_free_space.erase(it);
		} else {
			it++;
		}
	}

	return true;
}

void FixedSizeAllocator::FinalizeVacuum() {

	// free all (now unused) buffers
	while (min_vacuum_buffer_id < buffers.size()) {
		allocator.FreeData(buffers.back().ptr, BUFFER_ALLOC_SIZE);
		buffers.pop_back();
	}
}

Node FixedSizeAllocator::VacuumPointer(const Node ptr) {

	// we do not need to adjust the bitmask of the old buffer, because we will free the entire
	// buffer after the vacuum operation

	auto new_ptr = New();

	// new increases the allocation count
	total_allocations--;

	memcpy(Get(new_ptr), Get(ptr), allocation_size);
	return new_ptr;
}

void FixedSizeAllocator::Verify() const {
#ifdef DEBUG
	auto total_available_allocations = allocations_per_buffer * buffers.size();
	D_ASSERT(total_available_allocations >= total_allocations);
	D_ASSERT(buffers.size() >= buffers_with_free_space.size());
#endif
}

uint32_t FixedSizeAllocator::GetOffset(ValidityMask &mask, const idx_t allocation_count) {

	auto data = mask.GetData();

	// fills up a buffer sequentially before searching for free bits
	if (mask.RowIsValid(allocation_count)) {
		mask.SetInvalid(allocation_count);
		return allocation_count;
	}

	// get an entry with free bits
	for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
		if (data[entry_idx] != 0) {

			// find the position of the free bit
			auto entry = data[entry_idx];
			idx_t first_valid_bit = 0;

			// this loop finds the position of the rightmost set bit in entry and stores it
			// in first_valid_bit
			for (idx_t i = 0; i < 6; i++) {
				// set the left half of the bits of this level to zero and test if the entry is still not zero
				if (entry & BASE[i]) {
					// first valid bit is in the rightmost s[i] bits
					// permanently set the left half of the bits to zero
					entry &= BASE[i];
				} else {
					// first valid bit is in the leftmost s[i] bits
					// shift by s[i] for the next iteration and add s[i] to the position of the rightmost set bit
					entry >>= SHIFT[i];
					first_valid_bit += SHIFT[i];
				}
			}
			D_ASSERT(entry);

			auto prev_bits = entry_idx * sizeof(validity_t) * 8;
			D_ASSERT(mask.RowIsValid(prev_bits + first_valid_bit));
			mask.SetInvalid(prev_bits + first_valid_bit);
			return (prev_bits + first_valid_bit);
		}
	}

	throw InternalException("Invalid bitmask of FixedSizeAllocator");
}

} // namespace duckdb
