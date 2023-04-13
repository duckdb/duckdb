#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

shared_ptr<BlockHandle> BlockManager::RegisterBlock(block_id_t block_id, bool is_meta_block) {
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
	auto result = make_shared<BlockHandle>(*this, block_id);
	// for meta block, cache the handle in meta_blocks
	if (is_meta_block) {
		meta_blocks[block_id] = result;
	}
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

void BlockManager::ClearMetaBlockHandles() {
	meta_blocks.clear();
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(block_id_t block_id, shared_ptr<BlockHandle> old_block) {

	// pin the old block to ensure we have it loaded in memory
	auto old_handle = buffer_manager.Pin(old_block);
	D_ASSERT(old_block->state == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->buffer);

	// Temp buffers can be larger than the storage block size. But persistent buffers
	// cannot.
	D_ASSERT(old_block->buffer->AllocSize() <= Storage::BLOCK_ALLOC_SIZE);

	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->state == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->readers == 0);

	// move the data from the old block into data for the new block
	new_block->state = BlockState::BLOCK_LOADED;
	new_block->buffer = CreateBlock(block_id, old_block->buffer.get());
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

	buffer_manager.GetBufferPool().AddToEvictionQueue(new_block);

	return new_block;
}

void BlockManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: buffer could have been offloaded to disk: remove the file
		buffer_manager.DeleteTemporaryFile(block_id);
	} else {
		lock_guard<mutex> lock(blocks_lock);
		// on-disk block: erase from list of blocks in manager
		blocks.erase(block_id);
	}
}

} // namespace duckdb
