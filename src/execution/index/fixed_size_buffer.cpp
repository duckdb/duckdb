#include "duckdb/execution/index/fixed_size_buffer.hpp"

#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager)
    : block_manager(block_manager), segment_count(0), dirty(false), vacuum(false), block_handle(nullptr) {

	auto &buffer_manager = block_manager.buffer_manager;
	buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &block_handle);
}

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const block_id_t &block_id)
    : block_manager(block_manager), segment_count(segment_count), dirty(false), vacuum(false) {

	D_ASSERT(block_id < MAXIMUM_BLOCK);
	block_handle = block_manager.RegisterBlock(block_id);
	D_ASSERT(BlockId() < MAXIMUM_BLOCK);
}

void FixedSizeBuffer::Destroy() {
	if (InMemory()) {
		buffer_handle.Destroy();
	}
	if (OnDisk()) {
		block_manager.MarkBlockAsFree(BlockId());
	}
}

void FixedSizeBuffer::Serialize() {

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

	if (!OnDisk()) {
		// temporary block - convert to persistent
		auto block_id = block_manager.GetFreeBlockId();
		D_ASSERT(block_id < MAXIMUM_BLOCK);
		block_handle = block_manager.ConvertToPersistent(block_id, std::move(block_handle));
		buffer_handle.Destroy();

	} else {
		// already a persistent block - only need to write it
		auto block_id = block_handle->BlockId();
		D_ASSERT(block_id < MAXIMUM_BLOCK);
		block_manager.Write(buffer_handle.GetFileBuffer(), block_id);
	}
}

void FixedSizeBuffer::Pin() {

	auto &buffer_manager = block_manager.buffer_manager;
	D_ASSERT(BlockId() < MAXIMUM_BLOCK);
	buffer_handle = BufferHandle(buffer_manager.Pin(block_handle));
}

} // namespace duckdb
