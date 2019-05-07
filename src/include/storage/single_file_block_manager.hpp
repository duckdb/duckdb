//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/single_file_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/block_manager.hpp"
#include "storage/block.hpp"
#include "common/file_system.hpp"

namespace duckdb {
class FileBuffer;

//! SingleFileBlockManager is a implementation for a BlockManager which manages blocks in a single file
class SingleFileBlockManager : public BlockManager {
public:
	SingleFileBlockManager(string path, bool read_only, bool create_new);
	//! Returns a pointer to the block of the given id
	unique_ptr<Block> GetBlock(block_id_t id) override;
	string GetBlockPath(block_id_t id);
	//! Creates a new Block and returns a pointer
	unique_ptr<Block> CreateBlock() override;
	//! Flushes the given block to disk, in this case we use the path and stores this block on its file.
	void Flush(unique_ptr<Block> &block) override;

	void WriteHeader(int64_t version, block_id_t meta_block) override;
private:
	//! The path where the file is stored
	string path;
	//! The file handle
	unique_ptr<FileHandle> handle;
	//! The buffer used to read/write to the headers
	unique_ptr<FileBuffer> header_buffer;
};
} // namespace duckdb