//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//! The version number of the database storage format
extern const uint64_t VERSION_NUMBER;
// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
// default to 256KB. (1 << 18)
#define BLOCK_SIZE 262144
//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default to
//! the page size, which is 4KB. (1 << 12)
#define HEADER_SIZE 4096

using block_id_t = int64_t;

#define INVALID_BLOCK -1

//! The MainHeader is the first header in the storage file. The MainHeader is typically written only once for a database
//! file.
struct MainHeader {
	//! The version of the database
	uint64_t version_number;
	//! The set of flags used by the database
	uint64_t flags[4];
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
};

} // namespace duckdb
