//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/buffered_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
class Allocator;
class AllocatedData;
struct FileHandle;

struct BufferedFileHandle : public WrappedFileHandle {
	DUCKDB_API BufferedFileHandle(unique_ptr<FileHandle> inner_handle, size_t start, size_t end, Allocator &allocator);
	DUCKDB_API ~BufferedFileHandle() override;

	DUCKDB_API int64_t Read(void *buffer, idx_t nr_bytes) override {
		throw InternalException("Unsupported");
	}
	DUCKDB_API void Read(void *buffer, idx_t nr_bytes, idx_t location) override;
	DUCKDB_API void Close() override {
		inner->Close();
	}
	DUCKDB_API void Truncate(int64_t new_size) override {
		buffered.Reset();
		start = 0;
		end = 0;
		inner->Truncate(new_size);
	}
	DUCKDB_API int64_t Write(void *buffer, idx_t nr_bytes) override {
		buffered.Reset();
		start = 0;
		end = 0;
		return inner->Write(buffer, nr_bytes);
	}
	DUCKDB_API void Write(void *buffer, idx_t nr_bytes, idx_t location) override {
		buffered.Reset();
		start = 0;
		end = 0;
		inner->Write(buffer, nr_bytes, location);
	}
	size_t start;
	size_t end;
	AllocatedData buffered;
};

} // namespace duckdb
