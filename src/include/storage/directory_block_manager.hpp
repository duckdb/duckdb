//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/directory_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/block_manager.hpp"
#include "storage/directory_block.hpp"
#include "storage/block.hpp"

namespace duckdb {

//! DirectoryBlockManager is a implementation for a BlockManager which manages blocks from multiple files
class DirectoryBlockManager : public BlockManager {
public:
	//! Constructor gets the path to read/store the block
	DirectoryBlockManager(string path) : directory_path(path) {
		data_buffer = unique_ptr<char[]>(new char[BLOCK_SIZE]);
	}
	//! Returns a pointer to the block of the given id
	unique_ptr<Block> GetBlock(block_id_t id) override;
	string GetBlockPath(block_id_t id);
	//! Creates a new Block and returns a pointer
	unique_ptr<Block> CreateBlock() override;
	//! Flushes the given block to disk, in this case we use the path and stores this block on its file.
	void Flush(unique_ptr<Block> &block) override;

	void WriteHeader(int64_t version, block_id_t meta_block) override;
private:
	//! The directory where blocks are stored
	string directory_path;
	//! The data of the block-> multiple columns (each column has the same type).
	unique_ptr<char[]> data_buffer;
	//! The current block id to start assigning blocks to
	block_id_t free_id = 0;
};
} // namespace duckdb