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
		blocks[block_id] = move(block);
	} else {
		result_block = entry->second.get();
	}
	return make_unique<BlockHandle>(*this, result_block, block_id);
}

void BufferManager::Unpin(block_id_t block) {
	// for now, unpinning does nothing

}
