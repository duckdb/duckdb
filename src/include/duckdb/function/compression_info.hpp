//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/compression_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

class CompressionInfo {
public:
	explicit CompressionInfo(BlockManager &block_manager) : block_manager(block_manager) {
	}

public:
	//! The size below which the segment is compacted on flushing.
	idx_t GetCompactionFlushLimit() const {
		return block_manager.GetBlockSize() / 5 * 4;
	}
	//! The block size for blocks using this compression.
	idx_t GetBlockSize() const {
		return block_manager.GetBlockSize();
	}

	//! The block header size for blocks using this compression.
	idx_t GetBlockHeaderSize() const {
		return block_manager.GetBlockHeaderSize();
	}

	BlockManager &GetBlockManager() const {
		return block_manager;
	}

private:
	BlockManager &block_manager;
};

} // namespace duckdb
