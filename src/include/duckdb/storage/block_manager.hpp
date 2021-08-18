//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;

//! BlockManager is an abstract representation to manage blocks on DuckDB. When writing or reading blocks, the
//! BlockManager creates and accesses blocks. The concrete types implements how blocks are stored.
class BlockManager {
public:
	virtual ~BlockManager() = default;

	virtual void StartCheckpoint() = 0;
	//! Creates a new block inside the block manager
	virtual unique_ptr<Block> CreateBlock(block_id_t block_id) = 0;
	//! Return the next free block id
	virtual block_id_t GetFreeBlockId() = 0;
	//! Returns whether or not a specified block is the root block
	virtual bool IsRootBlock(block_id_t root) = 0;
	//! Mark a block as "modified"; modified blocks are added to the free list after a checkpoint (i.e. their data is
	//! assumed to be rewritten)
	virtual void MarkBlockAsModified(block_id_t block_id) = 0;
	//! Increase the reference count of a block. The block should hold at least one reference before this method is
	//! called.
	virtual void IncreaseBlockReferenceCount(block_id_t block_id) = 0;
	//! Get the first meta block id
	virtual block_id_t GetMetaBlock() = 0;
	//! Read the content of the block from disk
	virtual void Read(Block &block) = 0;
	//! Writes the block to disk
	virtual void Write(FileBuffer &block, block_id_t block_id) = 0;
	//! Writes the block to disk
	void Write(Block &block) {
		Write(block, block.id);
	}
	//! Write the header; should be the final step of a checkpoint
	virtual void WriteHeader(DatabaseHeader header) = 0;

	//! Returns the number of total blocks
	virtual idx_t TotalBlocks() = 0;
	//! Returns the number of free blocks
	virtual idx_t FreeBlocks() = 0;

	static BlockManager &GetBlockManager(ClientContext &context);
	static BlockManager &GetBlockManager(DatabaseInstance &db);
};
} // namespace duckdb
