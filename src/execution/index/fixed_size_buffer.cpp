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

void PartialBlockForIndex::Flush(const idx_t free_space_left) {
	FlushInternal(free_space_left);
	block_handle = block_manager.ConvertToPersistent(state.block_id, std::move(block_handle));
	Clear();
}

void PartialBlockForIndex::Merge(PartialBlock &other, idx_t offset, idx_t other_size) {
	throw InternalException("no merge for PartialBlockForIndex");
}

void PartialBlockForIndex::Clear() {
	block_handle.reset();
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
	buffer_handle = buffer_manager.Allocate(MemoryTag::ART_INDEX, block_manager.GetBlockSize(), false, &block_handle);
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
		// we can have multiple readers on a pinned block, and unpinning the buffer handle
		// decrements the reader count on the underlying block handle (Destroy() unpins)
		buffer_handle.Destroy();
	}
	if (OnDisk()) {
		// marking a block as modified decreases the reference count of multi-use blocks
		block_manager.MarkBlockAsModified(block_pointer.block_id);
	}
}

void FixedSizeBuffer::Serialize(PartialBlockManager &partial_block_manager, const idx_t available_segments,
                                const idx_t segment_size, const idx_t bitmask_offset) {

	// we do not serialize a block that is already on disk and not in memory
	if (!InMemory()) {
		if (!OnDisk() || dirty) {
			throw InternalException("invalid or missing buffer in FixedSizeAllocator");
		}
		return;
	}

	// we do not serialize a block that is already on disk and not dirty
	if (!dirty && OnDisk()) {
		return;
	}

	// the allocation possibly changed
	SetAllocationSize(available_segments, segment_size, bitmask_offset);

	// the buffer is in memory, so we copied it onto a new buffer when pinning
	D_ASSERT(InMemory() && !OnDisk());

	// now we write the changes, first get a partial block allocation
	PartialBlockAllocation allocation =
	    partial_block_manager.GetBlockAllocation(NumericCast<uint32_t>(allocation_size));
	block_pointer.block_id = allocation.state.block_id;
	block_pointer.offset = allocation.state.offset;

	auto &buffer_manager = block_manager.buffer_manager;

	if (allocation.partial_block) {
		// copy to an existing partial block
		D_ASSERT(block_pointer.offset > 0);
		auto &p_block_for_index = allocation.partial_block->Cast<PartialBlockForIndex>();
		auto dst_handle = buffer_manager.Pin(p_block_for_index.block_handle);
		memcpy(dst_handle.Ptr() + block_pointer.offset, buffer_handle.Ptr(), allocation_size);
		SetUninitializedRegions(p_block_for_index, segment_size, block_pointer.offset, bitmask_offset);

	} else {
		// create a new block that can potentially be used as a partial block
		D_ASSERT(block_handle);
		D_ASSERT(!block_pointer.offset);
		auto p_block_for_index = make_uniq<PartialBlockForIndex>(allocation.state, block_manager, block_handle);
		SetUninitializedRegions(*p_block_for_index, segment_size, block_pointer.offset, bitmask_offset);
		allocation.partial_block = std::move(p_block_for_index);
	}

	partial_block_manager.RegisterPartialBlock(std::move(allocation));

	// resetting this buffer
	buffer_handle.Destroy();
	block_handle = block_manager.RegisterBlock(block_pointer.block_id);
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);

	// we persist any changes, so the buffer is no longer dirty
	dirty = false;
}

void FixedSizeBuffer::Pin() {
	auto &buffer_manager = block_manager.buffer_manager;
	D_ASSERT(block_pointer.IsValid());
	D_ASSERT(block_handle && block_handle->BlockId() < MAXIMUM_BLOCK);
	D_ASSERT(!dirty);

	buffer_handle = buffer_manager.Pin(block_handle);

	// we need to copy the (partial) data into a new (not yet disk-backed) buffer handle
	shared_ptr<BlockHandle> new_block_handle;
	auto new_buffer_handle =
	    buffer_manager.Allocate(MemoryTag::ART_INDEX, block_manager.GetBlockSize(), false, &new_block_handle);

	memcpy(new_buffer_handle.Ptr(), buffer_handle.Ptr() + block_pointer.offset, allocation_size);

	Destroy();
	buffer_handle = std::move(new_buffer_handle);
	block_handle = std::move(new_block_handle);
	block_pointer = BlockPointer();
}

uint32_t FixedSizeBuffer::GetOffset(const idx_t bitmask_count) {

	// get the bitmask data
	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	ValidityMask mask(bitmask_ptr);
	auto data = mask.GetData();

	// fills up a buffer sequentially before searching for free bits
	if (mask.RowIsValid(segment_count)) {
		mask.SetInvalid(segment_count);
		return UnsafeNumericCast<uint32_t>(segment_count);
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
		return UnsafeNumericCast<uint32_t>(prev_bits + first_valid_bit);
	}

	throw InternalException("Invalid bitmask for FixedSizeAllocator");
}

void FixedSizeBuffer::SetAllocationSize(const idx_t available_segments, const idx_t segment_size,
                                        const idx_t bitmask_offset) {

	if (dirty) {
		auto max_offset = GetMaxOffset(available_segments);
		allocation_size = max_offset * segment_size + bitmask_offset;
	}
}

uint32_t FixedSizeBuffer::GetMaxOffset(const idx_t available_segments) {

	// this function calls Get() on the buffer
	D_ASSERT(InMemory());

	// finds the maximum zero bit in a bitmask, and adds one to it,
	// so that max_offset * segment_size = allocated_size of this bitmask's buffer
	idx_t entry_size = sizeof(validity_t) * 8;
	idx_t bitmask_count = available_segments / entry_size;
	if (available_segments % entry_size != 0) {
		bitmask_count++;
	}
	auto max_offset = UnsafeNumericCast<uint32_t>(bitmask_count * sizeof(validity_t) * 8);
	auto bits_in_last_entry = available_segments % (sizeof(validity_t) * 8);

	// get the bitmask data
	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	const ValidityMask mask(bitmask_ptr);
	const auto data = mask.GetData();

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
	throw InternalException("tried to serialize empty buffer");
}

void FixedSizeBuffer::SetUninitializedRegions(PartialBlockForIndex &p_block_for_index, const idx_t segment_size,
                                              const idx_t offset, const idx_t bitmask_offset) {

	// this function calls Get() on the buffer
	D_ASSERT(InMemory());

	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	ValidityMask mask(bitmask_ptr);

	idx_t i = 0;
	idx_t max_offset = offset + allocation_size;
	idx_t current_offset = offset + bitmask_offset;
	while (current_offset < max_offset) {

		if (mask.RowIsValid(i)) {
			D_ASSERT(current_offset + segment_size <= max_offset);
			p_block_for_index.AddUninitializedRegion(current_offset, current_offset + segment_size);
		}
		current_offset += segment_size;
		i++;
	}
}

} // namespace duckdb
