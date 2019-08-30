//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/block_manager.hpp"

#include "common/unordered_map.hpp"

#include <mutex>

namespace duckdb {
class BufferManager;

struct BlockHandle {
	BlockHandle(BufferManager &manager, Block *block, block_id_t block_id);
	~BlockHandle();

	BufferManager &manager;
	//! The managed block
	Block *block;
	//! The block id of the block
	block_id_t block_id;
};

struct BufferEntry {
	BufferEntry(unique_ptr<Block> block) :
		block(move(block)), ref_count(1), prev(nullptr) { }

	//! The actual block
	unique_ptr<Block> block;
	//! The amount of references to this entry
	index_t ref_count;
	//! Next node
	unique_ptr<BufferEntry> next;
	//! Prev entry
	BufferEntry *prev;
};

class BufferList {
public:
	BufferList() : last(nullptr), count(0) {}
public:
	//! Removes the first element (root) from the buffer list and returns it, O(1)
	unique_ptr<BufferEntry> Pop();
	//! Erase the specified element from the list and returns it, O(1)
	unique_ptr<BufferEntry> Erase(BufferEntry *entry);
	//! Insert an entry to the back of the list
	void Append(unique_ptr<BufferEntry> entry);
private:
	//! Root pointer
	unique_ptr<BufferEntry> root;
	//! Pointer to last element in list
	BufferEntry *last;
	//! The amount of entries in the list
	index_t count;
};


//! The buffer manager is a
class BufferManager {
	friend struct BlockHandle;
public:
	BufferManager(BlockManager &manager, index_t maximum_memory);

	//! Pin a block id, returning a block handle holding a pointer to the block
	unique_ptr<BlockHandle> Pin(block_id_t block);
private:
	//! Unpin a block id
	void Unpin(block_id_t block);

	//! Evict the least recently used block from the buffer manager, or throws an exception if there are no blocks available to evict
	unique_ptr<Block> EvictBlock();
private:
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	index_t current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	index_t maximum_memory;
	//! The block manager
	BlockManager &manager;
	//! The lock for the set of blocks
	std::mutex block_lock;
	//! A mapping of block id -> BufferEntry
	unordered_map<block_id_t, BufferEntry*> blocks;
	//! A linked list of buffer entries that are in use
	BufferList used_list;
	//! LRU list of unused blocks
	BufferList lru;

};
} // namespace duckdb
