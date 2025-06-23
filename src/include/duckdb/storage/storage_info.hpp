//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

struct FileHandle;

//! The standard row group size
#define DEFAULT_ROW_GROUP_SIZE 122880ULL
//! The definition of an invalid block
#define INVALID_BLOCK (-1)
//! The maximum block id is 2^62
#define MAXIMUM_BLOCK 4611686018427388000LL

//! The default block allocation size.
#define DEFAULT_BLOCK_ALLOC_SIZE 262144ULL
//! The configurable block allocation size.
#ifndef DUCKDB_BLOCK_ALLOC_SIZE
#define DUCKDB_BLOCK_ALLOC_SIZE DEFAULT_BLOCK_ALLOC_SIZE
#endif
//! The default block header size.
#define DEFAULT_BLOCK_HEADER_STORAGE_SIZE 8ULL
//! The default block header size.
#define DEFAULT_ENCRYPTION_BLOCK_HEADER_SIZE 40ULL
//! The configurable block allocation size.
#ifndef DUCKDB_BLOCK_HEADER_STORAGE_SIZE
#define DUCKDB_BLOCK_HEADER_STORAGE_SIZE DEFAULT_BLOCK_HEADER_STORAGE_SIZE
#endif

using block_id_t = int64_t;

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static idx_t SECTOR_SIZE = 4096U;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static idx_t FILE_HEADER_SIZE = 4096U;
	//! The maximum row group size
	constexpr static const idx_t MAX_ROW_GROUP_SIZE = 1ULL << 30ULL;
	//! The minimum block allocation size. This is the minimum size we test in our nightly tests.
	constexpr static idx_t MIN_BLOCK_ALLOC_SIZE = 16384ULL;
	//! The maximum block allocation size. This is the maximum size currently supported by duckdb.
	constexpr static idx_t MAX_BLOCK_ALLOC_SIZE = 262144ULL;
	//! The default block header size for blocks written to storage.
	constexpr static idx_t DEFAULT_BLOCK_HEADER_SIZE = sizeof(idx_t);
	//! The default block header size for blocks written to storage.
	constexpr static idx_t MAX_BLOCK_HEADER_SIZE = 128ULL;
	//! Block header size for encrypted blocks (40 bytes)
	constexpr static idx_t ENCRYPTED_BLOCK_HEADER_SIZE = 40ULL;
	//! The default block size.
	constexpr static idx_t DEFAULT_BLOCK_SIZE = DEFAULT_BLOCK_ALLOC_SIZE - DEFAULT_BLOCK_HEADER_SIZE;

	//! Ensures that a user-provided block allocation size matches all requirements.
	static void VerifyBlockAllocSize(const idx_t block_alloc_size);
	static void VerifyBlockHeaderSize(const idx_t block_header_size);
};

//! The version number default, lower and upper bounds of the database storage format
extern const uint64_t VERSION_NUMBER;
extern const uint64_t VERSION_NUMBER_LOWER;
extern const uint64_t VERSION_NUMBER_UPPER;
string GetDuckDBVersion(idx_t version_number);
optional_idx GetStorageVersion(const char *version_string);
string GetStorageVersionName(idx_t serialization_version);
optional_idx GetSerializationVersion(const char *version_string);
vector<string> GetSerializationCandidates();

//! The MainHeader is the first header in the storage file. The MainHeader is typically written only once for a database
//! file.
struct MainHeader {
	static constexpr idx_t MAX_VERSION_SIZE = 32;
	static constexpr idx_t MAGIC_BYTE_SIZE = 4;
	static constexpr idx_t MAGIC_BYTE_OFFSET = Storage::DEFAULT_BLOCK_HEADER_SIZE;
	static constexpr idx_t FLAG_COUNT = 4;
	//! Indicates whether database is encrypted
	static constexpr uint64_t ENCRYPTED_DATABASE_FLAG = 1;
	//! Encryption key length
	static constexpr uint64_t DEFAULT_ENCRYPTION_KEY_LENGTH = 32;
	//! The magic bytes in front of the file should be "DUCK"
	static const char MAGIC_BYTES[];
	//! The canary should be "DUCKKEY"
	static const char CANARY[];
	//! The version of the database
	uint64_t version_number;
	//! The set of flags used by the database
	uint64_t flags[FLAG_COUNT];

	//! optional metadata for encryption
	//! only used if encryption flag is set
	static constexpr idx_t ENCRYPTION_METADATA_LEN = 8;
	static constexpr idx_t SALT_LEN = 16;
	//! The canary is  a known plaintext
	//! this is used for early detection of a wrong key
	static constexpr idx_t CANARY_BYTE_SIZE = 8;
	//! Nonce, IV (nonce + counter) and tag length
	static constexpr uint64_t AES_NONCE_LEN = 12;
	static constexpr uint64_t AES_IV_LEN = 16;
	static constexpr uint64_t AES_TAG_LEN = 16;

	static void CheckMagicBytes(FileHandle &handle);

	string LibraryGitDesc() {
		return string(char_ptr_cast(library_git_desc), 0, MAX_VERSION_SIZE);
	}
	string LibraryGitHash() {
		return string(char_ptr_cast(library_git_hash), 0, MAX_VERSION_SIZE);
	}

	bool IsEncrypted() const {
		return flags[0] == MainHeader::ENCRYPTED_DATABASE_FLAG;
	}

	void SetEncryptionMetadata(data_ptr_t source) {
		memset(encryption_metadata, 0, ENCRYPTION_METADATA_LEN);
		memcpy(encryption_metadata, source, ENCRYPTION_METADATA_LEN);
	}

	void SetSalt(data_ptr_t source) {
		memset(salt, 0, SALT_LEN);
		memcpy(salt, source, SALT_LEN);
	}

	void SetEncryptedCanary(data_ptr_t source) {
		memset(encrypted_canary, 0, CANARY_BYTE_SIZE);
		memcpy(encrypted_canary, source, CANARY_BYTE_SIZE);
	}

	data_ptr_t GetEncryptionMetadata() {
		return encryption_metadata;
	}

	data_ptr_t GetSalt() {
		return salt;
	}

	data_ptr_t GetEncryptedCanary() {
		return encrypted_canary;
	}

	void Write(WriteStream &ser);
	static MainHeader Read(ReadStream &source);

private:
	data_t library_git_desc[MAX_VERSION_SIZE];
	data_t library_git_hash[MAX_VERSION_SIZE];
	data_t encryption_metadata[ENCRYPTION_METADATA_LEN];
	data_t salt[SALT_LEN];
	data_t encrypted_canary[CANARY_BYTE_SIZE];
};

//! The DatabaseHeader contains information about the current state of the database. Every storage file has two
//! DatabaseHeaders. On startup, the DatabaseHeader with the highest iteration count is used as the active header. When
//! a checkpoint is performed, the active DatabaseHeader is switched by increasing the iteration count of the
//! DatabaseHeader.
struct DatabaseHeader {
	//! The iteration count, increases by 1 every time the storage is checkpointed.
	uint64_t iteration;
	//! A pointer to the initial meta block
	idx_t meta_block;
	//! A pointer to the block containing the free list
	idx_t free_list;
	//! The number of blocks that is in the file as of this database header. If the file is larger than BLOCK_SIZE *
	//! block_count any blocks appearing AFTER block_count are implicitly part of the free_list.
	uint64_t block_count;
	//! The allocation size of blocks in this database file. Defaults to default_block_alloc_size (DBConfig).
	idx_t block_alloc_size;
	//! The vector size of the database file
	idx_t vector_size;
	//! The serialization compatibility version
	idx_t serialization_compatibility;

	void Write(WriteStream &ser);
	static DatabaseHeader Read(const MainHeader &header, ReadStream &source);
};

//! Detect mismatching constant values when compiling

#if (DEFAULT_ROW_GROUP_SIZE % STANDARD_VECTOR_SIZE != 0)
#error The row group size must be a multiple of the vector size
#endif
#if (DEFAULT_ROW_GROUP_SIZE < STANDARD_VECTOR_SIZE)
#error Row groups must be able to hold at least one vector
#endif
#if (DEFAULT_BLOCK_ALLOC_SIZE & (DEFAULT_BLOCK_ALLOC_SIZE - 1) != 0)
#error The default block allocation size must be a power of two
#endif
#if (DUCKDB_BLOCK_ALLOC_SIZE & (DUCKDB_BLOCK_ALLOC_SIZE - 1) != 0)
#error The duckdb block allocation size must be a power of two
#endif

} // namespace duckdb
