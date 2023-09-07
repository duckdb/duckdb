#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PartialBlockForIndex
//===--------------------------------------------------------------------===//

void PartialBlockForIndex::AddUninitializedRegion(idx_t start, idx_t end) {
	throw InternalException("no uninitialized regions for PartialBlockForIndex");
}

void PartialBlockForIndex::Flush(idx_t free_space_left) {
	if (IsFlushed()) {
		throw InternalException("Flush called on PartialBlockForIndex that was already flushed");
	}
	auto buffer_handle = block_manager.buffer_manager.Pin(block_handle);
	if (free_space_left > 0) {
		memset(buffer_handle.Ptr() + Storage::BLOCK_SIZE - free_space_left, 0, free_space_left);
	}
	block_manager.Write(buffer_handle.GetFileBuffer(), block_handle->BlockId());
	Clear();
}

void PartialBlockForIndex::Merge(PartialBlock &other, idx_t offset, idx_t other_size) {
	throw InternalException("no merge for PartialBlockForIndex");
}

//===--------------------------------------------------------------------===//
// FixedSizeBuffer
//===--------------------------------------------------------------------===//

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager)
    : block_manager(block_manager), segment_count(0), allocation_size(0), dirty(false), vacuum(false), block_pointer(),
      block_handle(nullptr) {

	auto &buffer_manager = block_manager.buffer_manager;
	buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &block_handle);
}

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const idx_t allocation_size,
                                 const BlockPointer &block_ptr)
    : block_manager(block_manager), segment_count(segment_count), allocation_size(allocation_size), dirty(false),
      vacuum(false), block_pointer(block_ptr) {

	D_ASSERT(block_ptr.IsValid());
	block_handle = block_manager.RegisterBlock(block_ptr.block_id);
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

	// the buffer is in memory
	D_ASSERT(InMemory());
	// the buffer never was on disk, or there were changes to it after loading it from disk
	D_ASSERT(!OnDisk() || dirty);

	// we persist any changes, so the buffer is no longer dirty
	dirty = false;

	if (OnDisk()) {
		// we only get here if the block is dirty AND already on disk
		// we always write new blocks (if dirty), so that we do not invalidate previous checkpoints
		block_manager.MarkBlockAsModified(block_pointer.block_id);
	}

	// now we write the changes, first get a partial block allocation
	PartialBlockAllocation allocation = partial_block_manager.GetBlockAllocation(allocation_size_p);
	block_pointer.block_id = allocation.state.block_id;
	block_pointer.offset = allocation.state.offset_in_block;
	allocation_size = allocation_size_p;

	auto &buffer_manager = block_manager.buffer_manager;

	// copy to a partial block
	if (allocation.partial_block) {
		block_handle = block_manager.RegisterBlock(block_pointer.block_id);
		auto p_buffer_handle = buffer_manager.Pin(block_handle);
		memcpy(p_buffer_handle.Ptr() + block_pointer.offset, buffer_handle.Ptr(), allocation_size);

	} else {
		D_ASSERT(block_handle);
		block_handle = block_manager.ConvertToPersistent(block_pointer.block_id, std::move(block_handle));
		allocation.partial_block = make_uniq<PartialBlockForIndex>(block_manager, allocation.state, block_handle);
	}

	buffer_handle.Destroy();
	partial_block_manager.RegisterPartialBlock(std::move(allocation));
}

void FixedSizeBuffer::Pin() {

	auto &buffer_manager = block_manager.buffer_manager;
	D_ASSERT(block_pointer.IsValid());
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);

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

} // namespace duckdb
