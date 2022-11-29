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

//! Handle for an allocated buffer, created from the BufferManager
class BufferHandle {
protected:
	DUCKDB_API BufferHandle() {
	}
	virtual DUCKDB_API ~BufferHandle() {
	}
	// disable copy constructors
	BufferHandle(const BufferHandle &other) = delete;
	BufferHandle &operator=(const BufferHandle &) = delete;

public:
	//! Returns whether or not the BufferHandle is valid.
	virtual DUCKDB_API bool IsValid() const = 0;
	//! Returns a pointer to the buffer data. Handle must be valid.
	virtual DUCKDB_API data_ptr_t Ptr() const = 0;
	//! Returns a pointer to the buffer data. Handle must be valid.
	virtual DUCKDB_API data_ptr_t Ptr() = 0;
	//! Gets the underlying file buffer. Handle must be valid.
	virtual DUCKDB_API FileBuffer &GetFileBuffer() = 0;
	//! Destroys the buffer handle
	virtual DUCKDB_API void Destroy() = 0;
	//! Get the internal block handle
	virtual const shared_ptr<BlockHandle> &GetBlockHandle() const = 0;
};

} // namespace duckdb
