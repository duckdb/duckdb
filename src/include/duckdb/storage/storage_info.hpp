//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {
struct FileHandle;

//! The standard row group size
#define STANDARD_ROW_GROUPS_SIZE 122880
//! The definition of an invalid block
#define INVALID_BLOCK (-1)
//! The maximum block id is 2^62
#define MAXIMUM_BLOCK 4611686018427388000LL
//! The default block size
#define DEFAULT_BLOCK_ALLOC_SIZE 262144

using block_id_t = int64_t;

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static idx_t SECTOR_SIZE = 4096;
	//! Block header size for blocks written to the storage
	constexpr static idx_t BLOCK_HEADER_SIZE = sizeof(uint64_t);
	//! Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
	//! default to 256KB. (1 << 18)
	constexpr static idx_t BLOCK_ALLOC_SIZE = DEFAULT_BLOCK_ALLOC_SIZE;
	//! The actual memory space that is available within the blocks
	constexpr static idx_t BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static idx_t FILE_HEADER_SIZE = 4096;
	//! The number of rows per row group (must be a multiple of the vector size)
	constexpr static const idx_t ROW_GROUP_SIZE = STANDARD_ROW_GROUPS_SIZE;
	//! The number of vectors per row group
	constexpr static const idx_t ROW_GROUP_VECTOR_COUNT = ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
};

//! The version number of the database storage format
extern const uint64_t VERSION_NUMBER;
const char *GetDuckDBVersion(idx_t version_number);

//! The MainHeader is the first header in the storage file. The MainHeader is typically written only once for a database
//! file.
struct MainHeader {
	static constexpr idx_t MAX_VERSION_SIZE = 32;
	static constexpr idx_t MAGIC_BYTE_SIZE = 4;
	static constexpr idx_t MAGIC_BYTE_OFFSET = Storage::BLOCK_HEADER_SIZE;
	static constexpr idx_t FLAG_COUNT = 4;
	//! The magic bytes in front of the file should be "DUCK"
	static const char MAGIC_BYTES[];
	//! The version of the database
	uint64_t version_number;
	//! The set of flags used by the database
	uint64_t flags[FLAG_COUNT];
	static void CheckMagicBytes(FileHandle &handle);

	string LibraryGitDesc() {
		return string((char *)library_git_desc, 0, MAX_VERSION_SIZE);
	}
	string LibraryGitHash() {
		return string((char *)library_git_hash, 0, MAX_VERSION_SIZE);
	}

	void Write(WriteStream &ser);
	static MainHeader Read(ReadStream &source);

private:
	data_t library_git_desc[MAX_VERSION_SIZE];
	data_t library_git_hash[MAX_VERSION_SIZE];
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
	//! The block size of the database file
	idx_t block_size;
	//! The vector size of the database file
	idx_t vector_size;

	void Write(WriteStream &ser);
	static DatabaseHeader Read(ReadStream &source);
};

//! Detect mismatching constant values when compiling

#if (STANDARD_ROW_GROUPS_SIZE % STANDARD_VECTOR_SIZE != 0)
#error The row group size must be a multiple of the vector size
#endif
#if (STANDARD_ROW_GROUPS_SIZE < STANDARD_VECTOR_SIZE)
#error Row groups must be able to hold at least one vector
#endif

static_assert(Storage::BLOCK_ALLOC_SIZE % Storage::SECTOR_SIZE == 0,
              "the block allocation size has to be a multiple of the sector size");
static_assert(Storage::BLOCK_SIZE < idx_t(NumericLimits<int32_t>::Maximum()),
              "the block size cannot exceed the maximum signed integer value,"
              "as some comparisons require casts");

} // namespace duckdb
