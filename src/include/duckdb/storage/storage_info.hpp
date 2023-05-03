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
class Serializer;
class Deserializer;
struct FileHandle;

#define STANDARD_ROW_GROUPS_SIZE 122880
#if STANDARD_ROW_GROUPS_SIZE < STANDARD_VECTOR_SIZE
#error Row groups should be able to hold at least one vector
#endif

#if ((STANDARD_ROW_GROUPS_SIZE % STANDARD_VECTOR_SIZE) != 0)
#error Row group size should be cleanly divisible by vector size
#endif

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

	void Serialize(Serializer &ser);
	static MainHeader Deserialize(Deserializer &source);
};

//! The DatabaseHeader contains information about the current state of the database. Every storage file has two
//! DatabaseHeaders. On startup, the DatabaseHeader with the highest iteration count is used as the active header. When
//! a checkpoint is performed, the active DatabaseHeader is switched by increasing the iteration count of the
//! DatabaseHeader.
struct DatabaseHeader {
	//! The iteration count, increases by 1 every time the storage is checkpointed.
	uint64_t iteration;
	//! A pointer to the initial meta block
	block_id_t meta_block;
	//! A pointer to the block containing the free list
	block_id_t free_list;
	//! The number of blocks that is in the file as of this database header. If the file is larger than BLOCK_SIZE *
	//! block_count any blocks appearing AFTER block_count are implicitly part of the free_list.
	uint64_t block_count;

	void Serialize(Serializer &ser);
	static DatabaseHeader Deserialize(Deserializer &source);
};

} // namespace duckdb
