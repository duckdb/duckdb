//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/system_buffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class BlockHandle;
class FileBuffer;

// The default implementation of BufferHandle used within the system
class SystemBufferHandle : public BufferHandle {
public:
	DUCKDB_API SystemBufferHandle();
	DUCKDB_API SystemBufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node);
	~SystemBufferHandle();
	DUCKDB_API SystemBufferHandle(SystemBufferHandle &&other) noexcept;
	DUCKDB_API SystemBufferHandle &operator=(SystemBufferHandle &&) noexcept;

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
	const shared_ptr<BlockHandle> &GetBlockHandle() const final override;

private:
	//! The block handle
	shared_ptr<BlockHandle> handle;
	//! The managed buffer node
	FileBuffer *node;
};

} // namespace duckdb
