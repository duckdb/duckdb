//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system/buffered_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class QueryContext;

//! Wraps a FileHandle with an optional prefetch range. Reads fully covered by
//! the realized buffer are served from memory; others pass through to the inner
//! handle. Read-only — writes go via ExtractInnerFileHandle.
//! v1: single hint slot. BufferedFileHandleState machine: EMPTY → HINT → REALIZED.
class BufferedFileHandle : public BaseFileHandle {
public:
	DUCKDB_API explicit BufferedFileHandle(unique_ptr<FileHandle> inner);
	DUCKDB_API ~BufferedFileHandle() override;

	BufferedFileHandle(const BufferedFileHandle &) = delete;
	BufferedFileHandle &operator=(const BufferedFileHandle &) = delete;

	//! Set the prefetch hint. No I/O. Replaces any prior hint or buffer.
	DUCKDB_API void RegisterPrefetch(idx_t offset, idx_t size);

	//! Copy the realized buffer from another wrapper (pending hints are not copied).
	DUCKDB_API void CopyBufferFrom(const BufferedFileHandle &other);

	//! Positional read; serves from the buffer when fully covered, else forwards
	//! to the inner handle (realizing a pending hint with one inner Read).
	DUCKDB_API void Read(void *buffer, idx_t nr_bytes, idx_t location);
	DUCKDB_API void ReadIntoBuffer(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location) override;

	DUCKDB_API idx_t GetFileSize() override;
	DUCKDB_API string GetPath() const override;

	//! Take ownership of the inner handle; the wrapper is drained afterwards.
	DUCKDB_API unique_ptr<FileHandle> ExtractInnerFileHandle();

private:
	enum class BufferedFileHandleState : uint8_t { EMPTY, HINT, REALIZED };

	unique_ptr<FileHandle> inner;
	BufferedFileHandleState state = BufferedFileHandleState::EMPTY;
	idx_t range_offset = 0;
	idx_t range_size = 0;
	unsafe_unique_array<char> buffer;
};

} // namespace duckdb
