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
class BufferManager;
class FileBuffer;

class BufferHandle {
public:
	BufferHandle(BufferManager &manager, block_id_t block_id, FileBuffer *node);
	~BufferHandle();

	BufferManager &manager;
	//! The block id of the block
	block_id_t block_id;
	//! The managed buffer node
	FileBuffer *node;
};

} // namespace duckdb
