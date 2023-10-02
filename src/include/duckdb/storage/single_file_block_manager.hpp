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
#include "duckdb/common/set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

class DatabaseInstance;
struct MetadataHandle;

struct StorageManagerOptions {
	bool read_only = false;
	bool use_direct_io = false;
	DebugInitialize debug_initialize = DebugInitialize::NO_INITIALIZE;
};

//! SingleFileBlockManager is an implementation for a BlockManager which manages blocks in a single file
class SingleFileBlockManager : public BlockManager {
	//! The location in the file where the block writing starts
	static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

public:
	SingleFileBlockManager(AttachedDatabase &db, string path, StorageManagerOptions options);

	void GetFileFlags(uint8_t &flags, FileLockType &lock, bool create_new);
	void CreateNewDatabase();
	void LoadExistingDatabase();

	//! Creates a new Block using the specified block_id and returns a pointer
	unique_ptr<Block> ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) override;
	unique_ptr<Block> CreateBlock(block_id_t block_id, FileBuffer *source_buffer) override;
	//! Return the next free block id
	block_id_t GetFreeBlockId() override;
	//! Returns whether or not a specified block is the root block
	bool IsRootBlock(MetaBlockPointer root) override;
	//! Mark a block as free (immediately re-writeable)
	void MarkBlockAsFree(block_id_t block_id) override;
	//! Mark a block as modified (re-writeable after a checkpoint)
	void MarkBlockAsModified(block_id_t block_id) override;
	//! Increase the reference count of a block. The block should hold at least one reference
	void IncreaseBlockReferenceCount(block_id_t block_id) override;
	//! Return the meta block id
	idx_t GetMetaBlock() override;
	//! Read the content of the block from disk
	void Read(Block &block) override;
	//! Write the given block to disk
	void Write(FileBuffer &block, block_id_t block_id) override;
	//! Write the header to disk, this is the final step of the checkpointing process
	void WriteHeader(DatabaseHeader header) override;
	//! Truncate the underlying database file after a checkpoint
	void Truncate() override;

	//! Returns the number of total blocks
	idx_t TotalBlocks() override;
	//! Returns the number of free blocks
	idx_t FreeBlocks() override;

private:
	//! Load the free list from the file
	void LoadFreeList();

	void Initialize(DatabaseHeader &header);

	void ReadAndChecksum(FileBuffer &handle, uint64_t location) const;
	void ChecksumAndWrite(FileBuffer &handle, uint64_t location) const;

	//! Return the blocks to which we will write the free list and modified blocks
	vector<MetadataHandle> GetFreeListBlocks();

private:
	AttachedDatabase &db;
	//! The active DatabaseHeader, either 0 (h1) or 1 (h2)
	uint8_t active_header;
	//! The path where the file is stored
	string path;
	//! The file handle
	unique_ptr<FileHandle> handle;
	//! The buffer used to read/write to the headers
	FileBuffer header_buffer;
	//! The list of free blocks that can be written to currently
	set<block_id_t> free_list;
	//! The list of multi-use blocks (i.e. blocks that have >1 reference in the file)
	//! When a multi-use block is marked as modified, the reference count is decreased by 1 instead of directly
	//! Appending the block to the modified_blocks list
	unordered_map<block_id_t, uint32_t> multi_use_blocks;
	//! The list of blocks that will be added to the free list
	unordered_set<block_id_t> modified_blocks;
	//! The current meta block id
	idx_t meta_block;
	//! The current maximum block id, this id will be given away first after the free_list runs out
	block_id_t max_block;
	//! The block id where the free list can be found
	idx_t free_list_id;
	//! The current header iteration count
	uint64_t iteration_count;
	//! The storage manager options
	StorageManagerOptions options;
	//! Lock for performing various operations in the single file block manager
	mutex block_lock;
};
} // namespace duckdb
