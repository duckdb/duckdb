#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PartialBlockForIndex
//===--------------------------------------------------------------------===//

PartialBlockForIndex::PartialBlockForIndex(PartialBlockState state, BlockManager &block_manager,
                                           const shared_ptr<BlockHandle> &block_handle)
    : PartialBlock(state, block_manager, block_handle) {
}

void PartialBlockForIndex::AddUninitializedRegion(idx_t start, idx_t end) {
	throw InternalException("no uninitialized regions for PartialBlockForIndex");
}

void PartialBlockForIndex::Flush(idx_t free_space_left) {
	auto buffer_handle = block_manager.buffer_manager.Pin(block_handle);
	if (free_space_left > 0) {
		memset(buffer_handle.Ptr() + Storage::BLOCK_SIZE - free_space_left, 0, free_space_left);
	}
	block_handle = block_manager.ConvertToPersistent(state.block_id, std::move(block_handle));
}

void PartialBlockForIndex::Merge(PartialBlock &other, idx_t offset, idx_t other_size) {
	throw InternalException("no merge for PartialBlockForIndex");
}

//===--------------------------------------------------------------------===//
// FixedSizeBuffer
//===--------------------------------------------------------------------===//

constexpr idx_t FixedSizeBuffer::BASE[];
constexpr uint8_t FixedSizeBuffer::SHIFT[];

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager)
    : block_manager(block_manager), segment_count(0), allocation_size(0), dirty(false), vacuum(false), block_pointer(),
      block_handle(nullptr) {

	auto &buffer_manager = block_manager.buffer_manager;
	buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &block_handle);
}

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const idx_t allocation_size,
                                 const BlockPointer &block_pointer)
    : block_manager(block_manager), segment_count(segment_count), allocation_size(allocation_size), dirty(false),
      vacuum(false), block_pointer(block_pointer) {

	D_ASSERT(block_pointer.IsValid());
	block_handle = block_manager.RegisterBlock(block_pointer.block_id);
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);
}

void FixedSizeBuffer::Destroy() {
	if (InMemory()) {
		// we can have multiple readers on a pinned block, and unpinning (Destroy unpins)
		// the buffer handle decrements the reader count on the underlying block handle
		buffer_handle.Destroy();
	}
	if (OnDisk()) {
		// marking a block as modified decreases the reference count of multi-use blocks
		block_manager.MarkBlockAsModified(block_pointer.block_id);
	}
}

void FixedSizeBuffer::Serialize(PartialBlockManager &partial_block_manager, const idx_t allocation_size_p) {

	if (!InMemory()) {
		if (!OnDisk() || dirty) {
			throw InternalException("invalid/missing buffer in FixedSizeAllocator");
		}
		return;
	}
	if (!dirty && OnDisk()) {
		return;
	}

	// the buffer is in memory, so we copied it onto a new buffer when pinning
	D_ASSERT(InMemory() && !OnDisk());
	D_ASSERT(buffer_handle.IsValid());
	// we persist any changes, so the buffer is no longer dirty
	dirty = false;

	// now we write the changes, first get a partial block allocation
	PartialBlockAllocation allocation = partial_block_manager.GetBlockAllocation(allocation_size_p);
	block_pointer.block_id = allocation.state.block_id;
	block_pointer.offset = allocation.state.offset;
	allocation_size = allocation_size_p;

	auto &buffer_manager = block_manager.buffer_manager;

	// copy to a partial block
	if (allocation.partial_block) {
		D_ASSERT(block_pointer.offset > 0);
		auto &p_block_for_index = allocation.partial_block->Cast<PartialBlockForIndex>();
		auto dst_handle = buffer_manager.Pin(p_block_for_index.block_handle);
		memcpy(dst_handle.Ptr() + block_pointer.offset, buffer_handle.Ptr(), allocation_size);

	} else {
		D_ASSERT(block_handle);
		allocation.partial_block = make_uniq<PartialBlockForIndex>(allocation.state, block_manager, block_handle);
	}

	buffer_handle.Destroy();
	partial_block_manager.RegisterPartialBlock(std::move(allocation));
	block_handle = block_manager.RegisterBlock(block_pointer.block_id);
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);
}

void FixedSizeBuffer::Pin() {

	auto &buffer_manager = block_manager.buffer_manager;
	D_ASSERT(block_pointer.IsValid());
	D_ASSERT(block_handle && block_handle->BlockId() < MAXIMUM_BLOCK);

	buffer_handle = buffer_manager.Pin(block_handle);

	// we need to copy the (partial) data into a new (not yet disk-backed) buffer handle
	shared_ptr<BlockHandle> new_block_handle;
	auto new_buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block_handle);

	memcpy(new_buffer_handle.Ptr(), buffer_handle.Ptr() + block_pointer.offset, allocation_size);

	Destroy();
	buffer_handle = std::move(new_buffer_handle);
	block_handle = new_block_handle;
	block_pointer.block_id = INVALID_BLOCK;
}

uint32_t FixedSizeBuffer::GetOffset(const idx_t bitmask_count) {

	// get the bitmask data
	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	ValidityMask mask(bitmask_ptr);
	auto data = mask.GetData();

	// fills up a buffer sequentially before searching for free bits
	if (mask.RowIsValid(segment_count)) {
		mask.SetInvalid(segment_count);
		return segment_count;
	}

	for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
		// get an entry with free bits
		if (data[entry_idx] == 0) {
			continue;
		}

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

	throw InternalException("Invalid bitmask for FixedSizeAllocator");
}

uint32_t FixedSizeBuffer::GetMaxOffset(const idx_t bitmask_count, const idx_t available_segments_per_buffer) {

	// finds the maximum free bit in a bitmask, and adds one to it,
	// so that max_offset * segment_size = allocated_size of this bitmask's buffer
	uint32_t max_offset = bitmask_count * sizeof(validity_t) * 8;
	auto bits_in_last_entry = available_segments_per_buffer % (sizeof(validity_t) * 8);

	// get the bitmask data
	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	ValidityMask mask(bitmask_ptr);
	auto data = mask.GetData();

	D_ASSERT(bitmask_count > 0);
	for (idx_t i = bitmask_count; i > 0; i--) {

		auto entry = data[i - 1];

		// set all bits after bits_in_last_entry
		if (i == bitmask_count) {
			entry |= ~idx_t(0) << bits_in_last_entry;
		}

		if (entry == ~idx_t(0)) {
			max_offset -= sizeof(validity_t) * 8;
			continue;
		}

		// invert data[entry_idx]
		auto entry_inv = ~entry;
		idx_t first_valid_bit = 0;

		// then find the position of the LEFTMOST set bit
		for (idx_t level = 0; level < 6; level++) {

			// set the right half of the bits of this level to zero and test if the entry is still not zero
			if (entry_inv & ~BASE[level]) {
				// first valid bit is in the leftmost s[level] bits
				// shift by s[level] for the next iteration and add s[level] to the position of the leftmost set bit
				entry_inv >>= SHIFT[level];
				first_valid_bit += SHIFT[level];
			} else {
				// first valid bit is in the rightmost s[level] bits
				// permanently set the left half of the bits to zero
				entry_inv &= BASE[level];
			}
		}
		D_ASSERT(entry_inv);
		max_offset -= sizeof(validity_t) * 8 - first_valid_bit;
		D_ASSERT(!mask.RowIsValid(max_offset));
		return max_offset + 1;
	}

	// there are no allocations in this buffer
	// FIXME: put this line back in and then fix the missing vacuum bug in
	// FIXME: test_index_large_aborted_append.test with force_restart
	//	throw InternalException("tried to serialize empty buffer");
	return 0;
}

} // namespace duckdb
