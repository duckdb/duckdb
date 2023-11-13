//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
struct FileHandle;

#define STANDARD_ROW_GROUPS_SIZE 122880
#if STANDARD_ROW_GROUPS_SIZE < STANDARD_VECTOR_SIZE
#error Row groups should be able to hold at least one vector
#endif

#if ((STANDARD_ROW_GROUPS_SIZE % STANDARD_VECTOR_SIZE) != 0)
#error Row group size should be cleanly divisible by vector size
#endif

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static int SECTOR_SIZE = 4096;
	//! Block header size for blocks written to the storage
	constexpr static int BLOCK_HEADER_SIZE = sizeof(uint64_t);
	// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
	// default to 256KB. (1 << 18)
	constexpr static int BLOCK_ALLOC_SIZE = 262144;
	//! The actual memory space that is available within the blocks
	constexpr static int BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static int FILE_HEADER_SIZE = 4096;
	//! The number of rows per row group (must be a multiple of the vector size)
	constexpr static const idx_t ROW_GROUP_SIZE = STANDARD_ROW_GROUPS_SIZE;
	//! The number of vectors per row group
	constexpr static const idx_t ROW_GROUP_VECTOR_COUNT = ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
};

//! The version number of the database storage format
extern const uint64_t VERSION_NUMBER;

const char *GetDuckDBVersion(idx_t version_number);

using block_id_t = int64_t;

#define INVALID_BLOCK (-1)

// maximum block id, 2^62
#define MAXIMUM_BLOCK 4611686018427388000LL

//! The MainHeader is the first header in the storage file. The MainHeader is typically written only once for a database
//! file.
struct MainHeader {
	static constexpr idx_t MAGIC_BYTE_SIZE = 4;
	static constexpr idx_t MAGIC_BYTE_OFFSET = Storage::BLOCK_HEADER_SIZE;
	static constexpr idx_t FLAG_COUNT = 4;
	// the magic bytes in front of the file
	// should be "DUCK"
	static const char MAGIC_BYTES[];
	//! The version of the database
	uint64_t version_number;
	//! The set of flags used by the database
	uint64_t flags[FLAG_COUNT];

	static void CheckMagicBytes(FileHandle &handle);

	void Write(WriteStream &ser);
	static MainHeader Read(ReadStream &source);
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

	void Write(WriteStream &ser);
	static DatabaseHeader Read(ReadStream &source);
};

} // namespace duckdb
