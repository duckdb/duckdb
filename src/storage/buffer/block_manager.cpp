#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"

namespace duckdb {

BlockManager::BlockManager(BufferManager &buffer_manager, const optional_idx block_alloc_size_p)
    : buffer_manager(buffer_manager), metadata_manager(make_uniq<MetadataManager>(*this, buffer_manager)),
      block_alloc_size(block_alloc_size_p) {
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

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(block_id_t block_id, shared_ptr<BlockHandle> old_block) {
	// pin the old block to ensure we have it loaded in memory
	auto old_handle = buffer_manager.Pin(old_block);
	D_ASSERT(old_block->state == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->buffer);

	// Temp buffers can be larger than the storage block size.
	// But persistent buffers cannot.
	D_ASSERT(old_block->buffer->AllocSize() <= GetBlockAllocSize());

	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->state == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->readers == 0);

	// move the data from the old block into data for the new block
	new_block->state = BlockState::BLOCK_LOADED;
	new_block->buffer = ConvertBlock(block_id, *old_block->buffer);
	new_block->memory_usage = old_block->memory_usage;
	new_block->memory_charge = std::move(old_block->memory_charge);

	// clear the old buffer and unload it
	old_block->buffer.reset();
	old_block->state = BlockState::BLOCK_UNLOADED;
	old_block->memory_usage = 0;
	old_handle.Destroy();
	old_block.reset();

	// persist the new block to disk
	Write(*new_block->buffer, block_id);

	// potentially purge the queue
	auto purge_queue = buffer_manager.GetBufferPool().AddToEvictionQueue(new_block);
	if (purge_queue) {
		buffer_manager.GetBufferPool().PurgeQueue(new_block->buffer->type);
	}

	return new_block;
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

void BlockManager::Truncate() {
}

} // namespace duckdb
