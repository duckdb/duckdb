#include "duckdb/storage/block_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"

namespace duckdb {

BlockManager::BlockManager(BufferManager &buffer_manager, const optional_idx block_alloc_size_p,
                           const optional_idx block_header_size_p)
    : buffer_manager(buffer_manager), metadata_manager(make_uniq<MetadataManager>(*this, buffer_manager)),
      block_alloc_size(block_alloc_size_p), block_header_size(block_header_size_p) {
}

shared_ptr<BlockHandle> BlockManager::RegisterBlock(block_id_t block_id) {
	lock_guard<mutex> lock(blocks_lock);
	// check if the block already exists
	auto entry = blocks.find(block_id);
	if (entry != blocks.end()) {
		// already exists: check if it hasn't expired yet
		auto existing_ptr = entry->second.lock();
		if (existing_ptr) {
			//! it hasn't! return it
			return existing_ptr;
		}
	}
	// create a new block pointer for this block
	auto result = make_shared_ptr<BlockHandle>(*this, block_id, MemoryTag::BASE_TABLE);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(QueryContext context, block_id_t block_id,
                                                          shared_ptr<BlockHandle> old_block, BufferHandle old_handle,
                                                          ConvertToPersistentMode mode) {
	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->GetState() == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->Readers() == 0);

	if (mode == ConvertToPersistentMode::THREAD_SAFE) {
		// safe mode - create a copy of the old block and operate on that
		// this ensures we don't modify the old block - which allows other concurrent operations on the old block to
		// continue
		auto old_block_copy = buffer_manager.AllocateMemory(old_block->GetMemoryTag(), this, false);
		auto copy_pin = buffer_manager.Pin(old_block_copy);
		memcpy(copy_pin.Ptr(), old_handle.Ptr(), GetBlockSize());
		old_block = std::move(old_block_copy);
		old_handle = std::move(copy_pin);
	}

	auto lock = old_block->GetLock();
	D_ASSERT(old_block->GetState() == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->GetBuffer(lock));
	if (old_block->Readers() > 1) {
		throw InternalException(
		    "BlockManager::ConvertToPersistent in destructive mode - cannot be called for block %d as old_block has "
		    "multiple readers active",
		    block_id);
	}

	// Temp buffers can be larger than the storage block size.
	// But persistent buffers cannot.
	D_ASSERT(old_block->GetBuffer(lock)->AllocSize() <= GetBlockAllocSize());

	// convert the buffer to a block
	auto converted_buffer = ConvertBlock(block_id, *old_block->GetBuffer(lock));

	// persist the new block to disk
	Write(context, *converted_buffer, block_id);

	// now convert the actual block
	old_block->ConvertToPersistent(lock, *new_block, std::move(converted_buffer));

	// destroy the old buffer
	lock.unlock();
	old_handle.Destroy();
	old_block.reset();

	// potentially purge the queue
	auto purge_queue = buffer_manager.GetBufferPool().AddToEvictionQueue(new_block);
	if (purge_queue) {
		buffer_manager.GetBufferPool().PurgeQueue(*new_block);
	}
	return new_block;
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(QueryContext context, block_id_t block_id,
                                                          shared_ptr<BlockHandle> old_block,
                                                          ConvertToPersistentMode mode) {
	// pin the old block to ensure we have it loaded in memory
	auto handle = buffer_manager.Pin(old_block);
	return ConvertToPersistent(context, block_id, std::move(old_block), std::move(handle), mode);
}

void BlockManager::UnregisterBlock(block_id_t id) {
	D_ASSERT(id < MAXIMUM_BLOCK);
	lock_guard<mutex> lock(blocks_lock);
	// on-disk block: erase from list of blocks in manager
	blocks.erase(id);
}

void BlockManager::UnregisterBlock(BlockHandle &block) {
	auto id = block.BlockId();
	if (id >= MAXIMUM_BLOCK) {
		// in-memory buffer: buffer could have been offloaded to disk: remove the file
		buffer_manager.DeleteTemporaryFile(block);
	} else {
		lock_guard<mutex> lock(blocks_lock);
		// on-disk block: erase from list of blocks in manager
		blocks.erase(id);
	}
}

MetadataManager &BlockManager::GetMetadataManager() {
	return *metadata_manager;
}

void BlockManager::Write(QueryContext context, FileBuffer &block, block_id_t block_id) {
	// Fallback to the old Write.
	Write(block, block_id);
}

void BlockManager::Truncate() {
}

} // namespace duckdb
