#include "duckdb/execution/index/fixed_size_buffer.hpp"

namespace duckdb {

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager)
    : block_manager(block_manager), segment_count(0), dirty(false), vacuum(false), block_handle(nullptr) {
	auto &buffer_manager = block_manager.buffer_manager;
	buffer_handle = make_uniq<BufferHandle>(buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE, false, &block_handle));
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);
}

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const block_id_t &block_id)
    : block_manager(block_manager), segment_count(segment_count), dirty(false), vacuum(false), buffer_handle(nullptr) {
	D_ASSERT(block_id < MAXIMUM_BLOCK);
	block_handle = block_manager.RegisterBlock(block_id);
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);
}

FixedSizeBuffer::~FixedSizeBuffer() {
	//	if (InMemory()) {
	//		buffer_handle->Destroy();
	//	}
	//	if (OnDisk()) {
	//		block_manager.UnregisterBlock(block_handle->BlockId(), true);
	//	}
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

	// first time writing to disk
	if (!OnDisk()) {
		auto block_id = block_manager.GetFreeBlockId();
		block_handle = block_manager.RegisterBlock(block_id);
		D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);
		block_manager.Write(buffer_handle->GetFileBuffer(), block_id);
		return;
	}

	// overwrite block on disk with changes
	auto block_id = block_handle->BlockId();
	D_ASSERT(block_id < MAXIMUM_BLOCK);
	block_manager.Write(buffer_handle->GetFileBuffer(), block_id);
}

void FixedSizeBuffer::Deserialize() {

	auto &buffer_manager = block_manager.buffer_manager;
	buffer_handle = make_uniq<BufferHandle>(buffer_manager.Pin(block_handle));
}

} // namespace duckdb
