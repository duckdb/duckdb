//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/in_memory_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

//! InMemoryBlockManager is an implementation for a BlockManager
class InMemoryBlockManager : public BlockManager {
public:
	using BlockManager::BlockManager;

	// LCOV_EXCL_START
	unique_ptr<Block> ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) override {
		throw InternalException("Cannot perform IO in in-memory database - ConvertBlock!");
	}
	unique_ptr<Block> CreateBlock(block_id_t block_id, FileBuffer *source_buffer) override {
		throw InternalException("Cannot perform IO in in-memory database - CreateBlock!");
	}
	block_id_t GetFreeBlockId() override {
		throw InternalException("Cannot perform IO in in-memory database - GetFreeBlockId!");
	}
	bool IsRootBlock(MetaBlockPointer root) override {
		throw InternalException("Cannot perform IO in in-memory database - IsRootBlock!");
	}
	void MarkBlockAsFree(block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - MarkBlockAsFree!");
	}
	void MarkBlockAsModified(block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - MarkBlockAsModified!");
	}
	void IncreaseBlockReferenceCount(block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - IncreaseBlockReferenceCount!");
	}
	idx_t GetMetaBlock() override {
		throw InternalException("Cannot perform IO in in-memory database - GetMetaBlock!");
	}
	void Read(Block &block) override {
		throw InternalException("Cannot perform IO in in-memory database - Read!");
	}
	void Write(FileBuffer &block, block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - Write!");
	}
	void WriteHeader(DatabaseHeader header) override {
		throw InternalException("Cannot perform IO in in-memory database - WriteHeader!");
	}
	idx_t TotalBlocks() override {
		throw InternalException("Cannot perform IO in in-memory database - TotalBlocks!");
	}
	idx_t FreeBlocks() override {
		throw InternalException("Cannot perform IO in in-memory database - FreeBlocks!");
	}
	// LCOV_EXCL_STOP
};
} // namespace duckdb
