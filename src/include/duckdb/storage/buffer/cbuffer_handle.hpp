//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/cbuffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

class CBufferHandle : public BufferHandle {
public:
	CBufferHandle() {
	}
	~CBufferHandle() {
	}

public:
	//! Returns whether or not the BufferHandle is valid.
	DUCKDB_API bool IsValid() const final override;
	//! Returns a pointer to the buffer data. Handle must be valid.
	DUCKDB_API data_ptr_t Ptr() const final override;
	//! Returns a pointer to the buffer data. Handle must be valid.
	DUCKDB_API data_ptr_t Ptr() final override;
	//! Gets the underlying file buffer. Handle must be valid.
	DUCKDB_API FileBuffer &GetFileBuffer() final override;
	//! Destroys the buffer handle
	DUCKDB_API void Destroy() final override;
	//! Get the internal block handle
	const shared_ptr<BlockHandle> &GetBlockHandle() const final override;

private:
	//! The allocated chunk of data that this buffer holds
	data_ptr_t data;
	//! The size of the allocated chunk of data
	idx_t allocation_size;

private:
};

} // namespace duckdb
