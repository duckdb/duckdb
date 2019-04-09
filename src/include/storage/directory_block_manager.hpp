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

//! DirectoryBlockManager is a implementation for a BlockManager which manages blocks from multiple file
class DirectoryBlockManager : public BlockManager {
public:
	//! Constructor gets the path to read/store the block
	explicit DirectoryBlockManager(string path) : directory_path(path) {
		data_buffer = unique_ptr<char[]>(new char[BLOCK_SIZE]);
	}
	DirectoryBlockManager() = default;
	//! Returns a pointer to the block of the given id
	unique_ptr<Block> GetBlock(block_id_t id) override;
	string GetBlockPath(block_id_t id) {
		return directory_path + "/" + std::to_string(id) + ".duck";
	}
	//! Creates a new Block and returns a pointer
	unique_ptr<Block> CreateBlock() override;
	//! Flushes the given block to disk, in this case we use the path and stores this block on its file.
	void Flush(unique_ptr<Block> &block) override;

	block_id_t GetMetadataBlockId() override {
		return data_block_id;
	}
	void SetMetadataBlockId(block_id_t block_id) override {
		data_block_id = block_id;
	}

	void AppendDataToBlock(DataChunk &chunk, unique_ptr<Block> &block, MetaBlockWriter &meta_writer) override;
	void LoadTableData(DataTable &table, DataChunk &chunk, unique_ptr<Block> meta_block) override;

private:
	//! The directory where blocks are stored
	string directory_path;
	//! Keeps track of the next avalible id for a new block
	block_id_t free_id{0};
	block_id_t data_block_id{1};
	//! The data of the block-> multiple columns (each column has the same type).
	unique_ptr<char[]> data_buffer;
};
} // namespace duckdb