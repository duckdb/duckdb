//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/block.hpp"

namespace duckdb {
//! BlockManager is an abstract representation to manage blocks on DuckDB. When writing or reading blocks, the
//! BlockManager creates and accesses blocks. The concrete types implements how blocks are stored.
class BlockManager {
public:
	virtual ~BlockManager() = default;

	//! Fetches an existing block by its ID
	virtual unique_ptr<Block> GetBlock(block_id_t id) = 0;
	//! Creates a new block inside the block manager
	virtual unique_ptr<Block> CreateBlock() = 0;
	//! Flushes a block to disk
	virtual void Flush(unique_ptr<Block> &block) = 0;
	//! Write the header; should be the final step of a checkpoint
	virtual void WriteHeader(int64_t version, block_id_t meta_block) = 0;
};
} // namespace duckdb