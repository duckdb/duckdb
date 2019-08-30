#include "storage/buffer_manager.hpp"

#include  "common/exception.hpp"

using namespace duckdb;
using namespace std;


BlockHandle::BlockHandle(BufferManager &manager, Block *block, block_id_t block_id) :
		manager(manager), block(block), block_id(block_id) {

}

BlockHandle::~BlockHandle() {
	manager.Unpin(block_id);
}

unique_ptr<BufferEntry> BufferList::Pop() {
	if (!root) {
		// no root: return nullptr
		return nullptr;
	}
	// fetch root
	auto entry = move(root);
	root = move(entry->next);
	if (!root) {
		// no root left: list is empty, last is nullptr
		last = nullptr;
	}
	count--;
	return entry;
}

unique_ptr<BufferEntry> BufferList::Erase(BufferEntry *entry) {
	assert(entry->prev || entry == root.get());
	assert(entry->next || entry == last);
	// first get the entry, either from the previous entry or from the root node
	auto current = entry->prev ? move(entry->prev->next) : move(root);
	auto prev = entry->prev;
	if (entry == last) {
		// entry was last entry: last is now the previous entry
		last = prev;
	}
	// now set up prev/next pointers correctly
	auto next = move(entry->next);
	if (!prev) {
		// no prev: entry was root
		root = move(next);
		if (root) {
			// new root no longer has prev pointer
			root->prev = nullptr;
		}
	} else if (prev != last) {
		assert(next);
		next->prev = prev;
		prev->next = move(next);
	}
	count--;
	return current;
}

void BufferList::Append(unique_ptr<BufferEntry> entry) {
	assert(!entry->next);
	if (!last) {
		// empty list: set as root
		entry->prev = nullptr;
		root = move(entry);
		last = root.get();
	} else {
		// non-empty list: append to last entry and set entry as last
		entry->prev = last;
		last->next = move(entry);
		last = last->next.get();
	}
	count++;
}

BufferManager::BufferManager(BlockManager &manager, index_t maximum_memory) :
	current_memory(0), maximum_memory(maximum_memory), manager(manager) {

}

unique_ptr<BlockHandle> BufferManager::Pin(block_id_t block_id) {
	// first obtain a lock on the set of blocks
	lock_guard<mutex> lock(block_lock);
	// now check if the block is already loaded
	Block *result_block;
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) {
		// block is not loaded, load the block
		current_memory += BLOCK_SIZE;
		unique_ptr<Block> block;
		if (current_memory > maximum_memory) {
			// not enough memory to hold the block: have to evict a block first
			block = EvictBlock();
			// take over the evicted block and use it to hold this block
			block->id = block_id;
		} else {
			// enough memory to create a new block: allocate it
			block = make_unique<Block>(block_id);
		}
		manager.Read(*block);
		result_block = block.get();
		// create a new buffer entry for this block
		auto buffer_entry = make_unique<BufferEntry>(move(block));
		// insert it into the block list
		blocks.insert(make_pair(block_id, buffer_entry.get()));
		used_list.Append(move(buffer_entry));
	} else {
		result_block = entry->second->block.get();
		// add one to the reference count
		entry->second->ref_count++;
		if (entry->second->ref_count == 1) {
			// ref count is 1, that means it used to be 0 (unused)
			// move from lru to used_list
			auto current_entry = lru.Erase(entry->second);
			used_list.Append(move(current_entry));
		}
	}
	return make_unique<BlockHandle>(*this, result_block, block_id);
}

void BufferManager::Unpin(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	// first find the block in the set of blocks
	auto entry = blocks.find(block_id);
	assert(entry != blocks.end());

	auto buffer_entry = entry->second;
	// then decerase the ref count
	assert(buffer_entry->ref_count > 0);
	buffer_entry->ref_count--;
	if (buffer_entry->ref_count == 0) {
		// no references left: move block out of used list and into lru list
		auto entry = used_list.Erase(buffer_entry);
		lru.Append(move(entry));
	}
}

unique_ptr<Block> BufferManager::EvictBlock() {
	// pop the first entry from the lru list
	auto entry = lru.Pop();
	if (!entry) {
		throw Exception("Not enough memory to complete operation!");
	}
	assert(entry->ref_count == 0);
	// erase this identifier from the set of blocks
	blocks.erase(entry->block->id);
	// free up the memory
	current_memory -= BLOCK_SIZE;
	// finally return the block obtained from the current entry
	return move(entry->block);
}
