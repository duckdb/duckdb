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

//! The buffer manager is a
class BufferManager {
	friend struct BlockHandle;
public:
	BufferManager(BlockManager &manager);

	//! Pin a block id, returning a block handle holding a pointer to the block
	unique_ptr<BlockHandle> Pin(block_id_t block);
private:
	//! Unpin a block id
	void Unpin(block_id_t block);
private:
	//! The block manager
	BlockManager &manager;
	//! The lock for the set of blocks
	std::mutex block_lock;
	//! A mapping of block id -> block
	unordered_map<block_id_t, unique_ptr<Block>> blocks;
};
} // namespace duckdb
