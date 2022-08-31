//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/managed_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class DatabaseInstance;

//! Managed buffer is an arbitrarily-sized buffer that is at least of size >= BLOCK_SIZE
class ManagedBuffer : public FileBuffer {
public:
	ManagedBuffer(DatabaseInstance &db, idx_t size, bool can_destroy, block_id_t id);
	ManagedBuffer(DatabaseInstance &db, FileBuffer &source, bool can_destroy, block_id_t id);

	DatabaseInstance &db;
	//! Whether or not the managed buffer can be freely destroyed when unpinned.
	//! - If can_destroy is true, the buffer can be destroyed when unpinned and hence be unrecoverable. After being
	//! destroyed, Pin() will return false.
	//! - If can_destroy is false, the buffer will instead be written to a temporary file on disk when unloaded from
	//! memory, and read back into memory when Pin() is called.
	bool can_destroy;
	//! The internal id of the buffer
	block_id_t id;
};

} // namespace duckdb
