//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class BlockHandle;
class BufferManager;
class ClientContext;
class DatabaseInstance;
class MetadataManager;

//! BlockManager is an abstract representation to manage blocks on DuckDB. When writing or reading blocks, the
//! BlockManager creates and accesses blocks. The concrete types implements how blocks are stored.
class BlockManager {
public:
	explicit BlockManager(BufferManager &buffer_manager);
	virtual ~BlockManager() = default;

	//! The buffer manager
	BufferManager &buffer_manager;

public:
	//! Creates a new block inside the block manager
	virtual unique_ptr<Block> ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) = 0;
	virtual unique_ptr<Block> CreateBlock(block_id_t block_id, FileBuffer *source_buffer) = 0;
	//! Return the next free block id
	virtual block_id_t GetFreeBlockId() = 0;
	//! Returns whether or not a specified block is the root block
	virtual bool IsRootBlock(MetaBlockPointer root) = 0;
	//! Mark a block as "free"; free blocks are immediately added to the free list and can be immediately overwritten
	virtual void MarkBlockAsFree(block_id_t block_id) = 0;
	//! Mark a block as "modified"; modified blocks are added to the free list after a checkpoint (i.e. their data is
	//! assumed to be rewritten)
	virtual void MarkBlockAsModified(block_id_t block_id) = 0;
	//! Increase the reference count of a block. The block should hold at least one reference before this method is
	//! called.
	virtual void IncreaseBlockReferenceCount(block_id_t block_id) = 0;
	//! Get the first meta block id
	virtual idx_t GetMetaBlock() = 0;
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

	//! Truncate the underlying database file after a checkpoint
	virtual void Truncate();

	//! Register a block with the given block id in the base file
	shared_ptr<BlockHandle> RegisterBlock(block_id_t block_id);
	//! Convert an existing in-memory buffer into a persistent disk-backed block
	shared_ptr<BlockHandle> ConvertToPersistent(block_id_t block_id, shared_ptr<BlockHandle> old_block);

	void UnregisterBlock(block_id_t block_id, bool can_destroy);

	MetadataManager &GetMetadataManager();

private:
	//! The lock for the set of blocks
	mutex blocks_lock;
	//! A mapping of block id -> BlockHandle
	unordered_map<block_id_t, weak_ptr<BlockHandle>> blocks;
	//! The metadata manager
	unique_ptr<MetadataManager> metadata_manager;
};
} // namespace duckdb
