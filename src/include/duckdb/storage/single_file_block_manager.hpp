//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/single_file_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class BufferManager;
class FileBuffer;

//! SingleFileBlockManager is an implementation for a BlockManager which manages blocks in a single file
class SingleFileBlockManager : public BlockManager {
	//! The location in the file where the block writing starts
	static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

public:
	SingleFileBlockManager(FileSystem &fs, string path, bool read_only, bool create_new, bool use_direct_io);

	void StartCheckpoint() override;
	//! Creates a new Block and returns a pointer
	unique_ptr<Block> CreateBlock() override;
	//! Return the next free block id
	block_id_t GetFreeBlockId() override;
	//! Return the meta block id
	block_id_t GetMetaBlock() override;
	//! Read the content of the block from disk
	void Read(Block &block) override;
	//! Write the given block to disk
	void Write(FileBuffer &block, block_id_t block_id) override;
	//! Write the header to disk, this is the final step of the checkpointing process
	void WriteHeader(DatabaseHeader header) override;

	//! Load the free list from the file
	void LoadFreeList(BufferManager &manager);

private:
	void Initialize(DatabaseHeader &header);

private:
	//! The active DatabaseHeader, either 0 (h1) or 1 (h2)
	uint8_t active_header;
	//! The path where the file is stored
	string path;
	//! The file handle
	unique_ptr<FileHandle> handle;
	//! The buffer used to read/write to the headers
	FileBuffer header_buffer;
	//! The list of free blocks that can be written to currently
	vector<block_id_t> free_list;
	//! The list of blocks that are used by the current block manager
	unordered_set<block_id_t> used_blocks;
	//! The current meta block id
	block_id_t meta_block;
	//! The current maximum block id, this id will be given away first after the free_list runs out
	block_id_t max_block;
	//! The block id where the free list can be found
	block_id_t free_list_id;
	//! The current header iteration count
	uint64_t iteration_count;
	//! Whether or not the db is opened in read-only mode
	bool read_only;
	//! Whether or not to use Direct IO to read the blocks
	bool use_direct_io;
};
} // namespace duckdb
