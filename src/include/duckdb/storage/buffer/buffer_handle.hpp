//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/buffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/file_buffer.hpp"

namespace duckdb {
class BlockHandle;
class FileBuffer;

class BufferHandle {
public:
	DUCKDB_API BufferHandle();
	DUCKDB_API BufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node);
	DUCKDB_API ~BufferHandle();
	// disable copy constructors
	BufferHandle(const BufferHandle &other) = delete;
	BufferHandle &operator=(const BufferHandle &) = delete;
	//! enable move constructors
	DUCKDB_API BufferHandle(BufferHandle &&other) noexcept;
	DUCKDB_API BufferHandle &operator=(BufferHandle &&) noexcept;

public:
	//! Returns whether or not the BufferHandle is valid.
	DUCKDB_API bool IsValid() const;
	//! Returns a pointer to the buffer data. Handle must be valid.
	inline data_ptr_t Ptr() const {
		D_ASSERT(IsValid());
		return node->buffer;
	}
	//! Returns a pointer to the buffer data. Handle must be valid.
	inline data_ptr_t Ptr() {
		D_ASSERT(IsValid());
		return node->buffer;
	}
	//! Gets the underlying file buffer. Handle must be valid.
	DUCKDB_API FileBuffer &GetFileBuffer();
	//! Destroys the buffer handle
	DUCKDB_API void Destroy();

	const shared_ptr<BlockHandle> &GetBlockHandle() const {
		return handle;
	}

private:
	//! The block handle
	shared_ptr<BlockHandle> handle;
	//! The managed buffer node
	FileBuffer *node;
};

} // namespace duckdb
