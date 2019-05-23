//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/block.hpp"
#include "storage/storage_info.hpp"

namespace duckdb {
//! BlockManager is an abstract representation to manage blocks on DuckDB. When writing or reading blocks, the
//! BlockManager creates and accesses blocks. The concrete types implements how blocks are stored.
class BlockManager {
public:
	virtual ~BlockManager() = default;

	//! Creates a new block inside the block manager
	virtual unique_ptr<Block> CreateBlock() = 0;
	//! Return the next free block id
	virtual block_id_t GetFreeBlockId() = 0;
	//! Get the first meta block id
	virtual block_id_t GetMetaBlock() = 0;
	//! Read the content of the block from disk
	virtual void Read(Block &block) = 0;
	//! Writes the block to disk
	virtual void Write(Block &block) = 0;
	//! Write the header; should be the final step of a checkpoint
	virtual void WriteHeader(DatabaseHeader header) = 0;
};
} // namespace duckdb
