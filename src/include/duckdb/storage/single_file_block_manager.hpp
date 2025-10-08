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
#include "duckdb/common/encryption_functions.hpp"

namespace duckdb {

class DatabaseInstance;
struct MetadataHandle;

struct EncryptionOptions {
	//! indicates whether the db is encrypted
	bool encryption_enabled = false;
	//! Whether Additional Authenticated Data is used
	bool additional_authenticated_data = false;
	//! derived encryption key id
	string derived_key_id;
	// //! Cipher used for encryption
	// EncryptionTypes::CipherType cipher = EncryptionTypes::CipherType::INVALID;
	//! key derivation function (kdf) used
	EncryptionTypes::KeyDerivationFunction kdf = EncryptionTypes::KeyDerivationFunction::SHA256;
	//! Key Length
	uint32_t key_length = MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH;
	//! User key pointer (to StorageOptions)
	shared_ptr<string> user_key;
};

struct StorageManagerOptions {
	bool read_only = false;
	bool use_direct_io = false;
	DebugInitialize debug_initialize = DebugInitialize::NO_INITIALIZE;
	optional_idx block_alloc_size;
	optional_idx storage_version;
	optional_idx version_number;
	optional_idx block_header_size;
	//! Unique database identifier and optional encryption salt.
	data_t db_identifier[MainHeader::DB_IDENTIFIER_LEN];
	EncryptionOptions encryption_options;
};

//! SingleFileBlockManager is an implementation for a BlockManager which manages blocks in a single file
class SingleFileBlockManager : public BlockManager {
	//! The location in the file where the block writing starts
	static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

public:
	SingleFileBlockManager(AttachedDatabase &db_p, const string &path_p, const StorageManagerOptions &options_p);

	FileOpenFlags GetFileFlags(bool create_new) const;
	//! Creates a new database.
	void CreateNewDatabase(QueryContext context);
	//! Loads an existing database. We pass the provided block allocation size as a parameter
	//! to detect inconsistencies with the file header.
	void LoadExistingDatabase(QueryContext context);

	//! Creates a new Block using the specified block_id and returns a pointer
	unique_ptr<Block> ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) override;
	unique_ptr<Block> CreateBlock(block_id_t block_id, FileBuffer *source_buffer) override;
	//! Return the next free block id
	block_id_t GetFreeBlockId() override;
	//! Check the next free block id - but do not assign or allocate it
	block_id_t PeekFreeBlockId() override;
	//! Returns whether or not a specified block is the root block
	bool IsRootBlock(MetaBlockPointer root) override;
	//! Mark a block as free (immediately re-writeable)
	void MarkBlockAsFree(block_id_t block_id) override;
	//! Mark a block as used (no longer re-writeable)
	void MarkBlockAsUsed(block_id_t block_id) override;
	//! Mark a block as modified (re-writeable after a checkpoint)
	void MarkBlockAsModified(block_id_t block_id) override;
	//! Increase the reference count of a block. The block should hold at least one reference
	void IncreaseBlockReferenceCount(block_id_t block_id) override;
	//! Return the meta block id
	idx_t GetMetaBlock() override;
	//! Read the content of the block from disk
	void Read(QueryContext context, Block &block) override;

	//! Read individual blocks
	void ReadBlock(Block &block, bool skip_block_header = false) const;
	void ReadBlock(data_ptr_t internal_buffer, uint64_t block_size, bool skip_block_header = false) const;
	//! Read the content of a range of blocks into a buffer
	void ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t block_count) override;
	//! Write the block to disk. Use Write with client context instead.
	void Write(FileBuffer &buffer, block_id_t block_id) override;
	//! Write the block to disk.
	void Write(QueryContext context, FileBuffer &buffer, block_id_t block_id) override;
	//! Write the header to disk, this is the final step of the checkpointing process
	void WriteHeader(QueryContext context, DatabaseHeader header) override;
	//! Sync changes to the underlying file
	void FileSync() override;
	//! Truncate the underlying database file after a checkpoint
	void Truncate() override;

	bool InMemory() override {
		return false;
	}
	//! Returns the number of total blocks
	idx_t TotalBlocks() override;
	//! Returns the number of free blocks
	idx_t FreeBlocks() override;
	//! Whether or not the attached database is a remote file
	bool IsRemote() override;
	//! Whether or not to prefetch
	bool Prefetch() override;

	//! Return the checkpoint iteration of the file.
	uint64_t GetCheckpointIteration() const {
		return iteration_count;
	}
	//! Return the version number of the file.
	uint64_t GetVersionNumber() const;
	//! Return the database identifier.
	data_ptr_t GetDBIdentifier() {
		return options.db_identifier;
	}

private:
	//! Loads the free list of the file.
	void LoadFreeList(QueryContext context);

	//! Initializes the database header. We pass the provided block allocation size as a parameter
	//!	to detect inconsistencies with the file header.
	void Initialize(const DatabaseHeader &header, const optional_idx block_alloc_size);

	void CheckChecksum(FileBuffer &block, uint64_t location, uint64_t delta, bool skip_block_header = false) const;
	void CheckChecksum(data_ptr_t start_ptr, uint64_t delta, bool skip_block_header = false) const;

	void ReadAndChecksum(QueryContext context, FileBuffer &handle, uint64_t location,
	                     bool skip_block_header = false) const;
	void ChecksumAndWrite(QueryContext context, FileBuffer &handle, uint64_t location,
	                      bool skip_block_header = false) const;

	idx_t GetBlockLocation(block_id_t block_id) const;

	// Encrypt, Store, Decrypt the canary
	static void StoreEncryptedCanary(AttachedDatabase &db, MainHeader &main_header, const string &key_id);
	static void StoreDBIdentifier(MainHeader &main_header, const data_ptr_t db_identifier);
	void StoreEncryptionMetadata(MainHeader &main_header) const;

	//! Check and adding Encryption Keys
	void CheckAndAddEncryptionKey(MainHeader &main_header, string &user_key);
	void CheckAndAddEncryptionKey(MainHeader &main_header);

	//! Return the blocks to which we will write the free list and modified blocks
	vector<MetadataHandle> GetFreeListBlocks();
	void TrimFreeBlocks();

	void IncreaseBlockReferenceCountInternal(block_id_t block_id);

	//! Verify the block usage count
	void VerifyBlocks(const unordered_map<block_id_t, idx_t> &block_usage_count) override;

	void AddStorageVersionTag();

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
	//! The list of blocks that were freed since the last checkpoint.
	set<block_id_t> newly_freed_list;
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
	//! The current header iteration count.
	uint64_t iteration_count;
	//! The storage manager options
	StorageManagerOptions options;
	//! Lock for performing various operations in the single file block manager
	mutex block_lock;
};
} // namespace duckdb
