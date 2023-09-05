#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

FixedSizeBuffer::FixedSizeBuffer(PartialBlockManager &partial_block_manager)
    : partial_block_manager(partial_block_manager), segment_count(0), size(0), max_offset(0), dirty(false),
      vacuum(false), size_changed(false), block_pointer(), block_handle(nullptr) {

	auto &buffer_manager = partial_block_manager.GetBlockManager().buffer_manager;
	buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &block_handle);
}

FixedSizeBuffer::FixedSizeBuffer(PartialBlockManager &partial_block_manager, const idx_t segment_count,
                                 const idx_t size, const uint32_t max_offset, const BlockPointer &block_ptr)
    : partial_block_manager(partial_block_manager), segment_count(segment_count), size(size), max_offset(max_offset),
      dirty(false), vacuum(false), size_changed(false), block_pointer(block_ptr) {

	D_ASSERT(block_ptr.IsValid());
	block_handle = partial_block_manager.GetBlockManager().RegisterBlock(block_ptr.block_id);
}

void FixedSizeBuffer::Destroy() {
	if (InMemory()) {
		// we can have multiple readers on a pinned block, and unpinning (Destroy unpins)
		// the buffer handle decrements the reader count on the underlying block handle
		buffer_handle.Destroy();
	}
	if (OnDisk()) {
		// marking a block as modified decreases the reference count of multi-use blocks
		partial_block_manager.GetBlockManager().MarkBlockAsModified(block_pointer.block_id);
	}
}

void FixedSizeBuffer::Serialize() {

	if (!InMemory()) {
		if (!OnDisk() || dirty || size_changed) {
			throw InternalException("invalid/missing buffer in FixedSizeAllocator");
		}
		return;
	}
	if (!dirty && OnDisk()) {
		D_ASSERT(!size_changed);
		return;
	}

	// the buffer is in memory
	D_ASSERT(InMemory());
	// the buffer never was on disk, or there were changes to it after loading it from disk
	D_ASSERT(!OnDisk() || dirty);

	// we persist any changes, so the buffer is no longer dirty
	dirty = false;

	// TODO: uncomment and use?
	//	auto &block_manager = partial_block_manager.GetBlockManager();

	if (OnDisk()) {
		// we only get here if the block is dirty AND already on disk
		// because the block is dirty, its size possibly changed
		if (!size_changed) {

			// already a persistent block - only need to write to it (same size)
			auto block_id = block_handle->BlockId();
			D_ASSERT(block_pointer.IsValid() && block_id == block_pointer.block_id);
			// TODO: somehow overwrite the exact same partial block location?
			return;
		}

		// the size changed, so our partial block becomes invalid
		// TODO: free the partial block?
		// TODO: write to a new partial block (enter !OnDisk case)
	}

	// we persist any changes, and the current size is the size that we observe
	size_changed = false;

	// not yet on disk
	// temporary block - convert to persistent (partial) block
	// TODO: somehow use the PartialBlockManager here to write the partial block
	auto block_id = partial_block_manager.GetBlockManager().GetFreeBlockId();
	D_ASSERT(block_id < MAXIMUM_BLOCK);
	block_handle = partial_block_manager.GetBlockManager().ConvertToPersistent(block_id, std::move(block_handle));
	buffer_handle.Destroy();
}

void FixedSizeBuffer::Pin() {
	// this buffer is not the only one holding a buffer handle of this block,
	// if the block is a partial block with enough free space

	auto &buffer_manager = partial_block_manager.GetBlockManager().buffer_manager;
	D_ASSERT(block_pointer.IsValid());
	buffer_handle = BufferHandle(buffer_manager.Pin(block_handle));
}

} // namespace duckdb
