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

struct EncryptionOptions {

	enum CipherType : uint8_t { UNKNOWN = 0, GCM = 1, CTR = 2, CBC = 3 };

	enum KeyDerivationFunction : uint8_t { DEFAULT = 0, SHA256 = 1, PBKDF2 = 2 };

	string CipherToString(CipherType cipher_p) const {
		switch (cipher_p) {
		case GCM:
			return "gcm";
		case CTR:
			return "ctr";
		case CBC:
			return "cbc";
		default:
			return "unknown";
		}
	}

	string KDFToString(KeyDerivationFunction kdf_p) const {
		switch (kdf_p) {
		case SHA256:
			return "sha256";
		case PBKDF2:
			return "pbkdf2";
		default:
			return "default";
		}
	}

	KeyDerivationFunction StringToKDF(const string &key_derivation_function) const {
		if (key_derivation_function == "sha256") {
			return KeyDerivationFunction::SHA256;
		} else if (key_derivation_function == "pbkdf2") {
			return KeyDerivationFunction::PBKDF2;
		} else {
			return KeyDerivationFunction::DEFAULT;
		}
	}

	CipherType StringToCipher(const string &encryption_cipher) const {
		if (encryption_cipher == "gcm") {
			return CipherType::GCM;
		} else if (encryption_cipher == "ctr") {
			return CipherType::CTR;
		} else if (encryption_cipher == "cbc") {
			return CipherType::CBC;
		}
		return CipherType::UNKNOWN;
	}

	//! indicates whether the db is encrypted
	bool encryption_enabled = false;
	//! derived encryption key
	string derived_key;
	//! Cipher used for encryption
	CipherType cipher;
	//! key derivation function (kdf) used
	KeyDerivationFunction kdf = KeyDerivationFunction::SHA256;
	//! Key Length
	uint32_t key_length = MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH;
};

struct StorageManagerOptions {
	bool read_only = false;
	bool use_direct_io = false;
	DebugInitialize debug_initialize = DebugInitialize::NO_INITIALIZE;
	optional_idx block_alloc_size;
	optional_idx storage_version;
	optional_idx version_number;
	optional_idx block_header_size;

	EncryptionOptions encryption_options;
};

//! SingleFileBlockManager is an implementation for a BlockManager which manages blocks in a single file
class SingleFileBlockManager : public BlockManager {
	//! The location in the file where the block writing starts
	static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

public:
	SingleFileBlockManager(AttachedDatabase &db, const string &path, const StorageManagerOptions &options_p);

	FileOpenFlags GetFileFlags(bool create_new) const;
	//! Creates a new database.
	void CreateNewDatabase(string *encryption_key = nullptr);
	//! Loads an existing database. We pass the provided block allocation size as a parameter
	//! to detect inconsistencies with the file header.
	void LoadExistingDatabase(string *encryption_key = nullptr);

	//! Derive encryption key
	static string DeriveKey(const string &user_key, data_ptr_t salt = nullptr);

	//! Lock and unlock encryption key
	void LockEncryptionKey();
	void UnlockEncryptionKey();

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
	void Read(Block &block) override;
	//! Read the content of a range of blocks into a buffer
	void ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t block_count) override;
	//! Write the given block to disk
	void Write(FileBuffer &block, block_id_t block_id) override;
	//! Write the header to disk, this is the final step of the checkpointing process
	void WriteHeader(DatabaseHeader header) override;
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

private:
	//! Loads the free list of the file.
	void LoadFreeList();
	//! Initializes the database header. We pass the provided block allocation size as a parameter
	//!	to detect inconsistencies with the file header.
	void Initialize(const DatabaseHeader &header, const optional_idx block_alloc_size);

	void EncryptBuffer(FileBuffer &block, FileBuffer &temp_buffer_manager, uint64_t delta) const;
	void DecryptBuffer(FileBuffer &block, uint64_t delta) const;

	void ReadAndChecksum(FileBuffer &handle, uint64_t location, bool skip_block_header = false) const;
	void ChecksumAndWrite(FileBuffer &handle, uint64_t location, bool skip_block_header = false) const;

	idx_t GetBlockLocation(block_id_t block_id);

	//! Return the blocks to which we will write the free list and modified blocks
	vector<MetadataHandle> GetFreeListBlocks();
	void TrimFreeBlocks();

	void IncreaseBlockReferenceCountInternal(block_id_t block_id);

	//! Verify the block usage count
	void VerifyBlocks(const unordered_map<block_id_t, idx_t> &block_usage_count) override;

	void AddStorageVersionTag();
	uint64_t GetVersionNumber();

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
	//! The current header iteration count
	uint64_t iteration_count;
	//! The storage manager options
	StorageManagerOptions options;
	//! Lock for performing various operations in the single file block manager
	mutex block_lock;
};
} // namespace duckdb
