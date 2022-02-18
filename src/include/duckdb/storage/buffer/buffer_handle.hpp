//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/buffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class BlockHandle;
class FileBuffer;

class BufferHandle {
public:
	BufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node);
	DUCKDB_API ~BufferHandle();

	//! The block handle
	shared_ptr<BlockHandle> handle;
	//! The managed buffer node
	FileBuffer *node;
	DUCKDB_API data_ptr_t Ptr();
};

} // namespace duckdb
