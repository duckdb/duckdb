#include "storage/buffer_manager.hpp"

using namespace duckdb;
using namespace std;


BlockHandle::BlockHandle(BufferManager &manager, Block *block, block_id_t block_id) :
		manager(manager), block(block), block_id(block_id) {

}

BlockHandle::~BlockHandle() {
	manager.Unpin(block_id);
}


BufferManager::BufferManager(BlockManager &manager) :
	manager(manager) {

}

unique_ptr<BlockHandle> BufferManager::Pin(block_id_t block_id) {
	// first obtain a lock on the set of blocks
	lock_guard<mutex> lock(block_lock);
	// now check if the block is already loaded
	Block *result_block;
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		// block is not loaded, load the block
		auto block = make_unique<Block>(block_id);
		manager.Read(*block);
		result_block = block.get();
		// insert the block with 1 reference into the block pool
		blocks.insert(make_pair(block_id, BufferEntry(move(block))));
	} else {
		result_block = entry->second.block.get();
		entry->second.ref_count++;
	}
	return make_unique<BlockHandle>(*this, result_block, block_id);
}

void BufferManager::Unpin(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	// decrease ref count
	auto entry = blocks.find(block_id);
	assert(entry != blocks.end());
	assert(entry->second.ref_count > 0);
	entry->second.ref_count--;
	if (entry->second.ref_count == 0) {
		// no references left: erase block immediately (for now)
		blocks.erase(block_id);
	}
}
