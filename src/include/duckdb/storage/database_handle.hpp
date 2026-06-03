//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/database_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_options.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/memory_mapped_file.hpp"

namespace duckdb {
struct StorageManagerOptions;

enum class DatabaseOpenMode { OPEN_EXISTING_FILE, CREATE_NEW_FILE };

struct DatabaseHandle {
	explicit DatabaseHandle(unique_ptr<FileHandle> handle);
	explicit DatabaseHandle(unique_ptr<MemoryMappedFile> mmap_handle);

	static unique_ptr<DatabaseHandle> Open(AttachedDatabase &db, const string &path,
	                                       const StorageManagerOptions &options, DatabaseOpenMode open_mode);
	bool OnDiskFile() const;

	void CheckMagicBytes(QueryContext context);
	void Read(QueryContext context, FileBuffer &block, uint64_t location) const;
	void Write(QueryContext context, FileBuffer &block, uint64_t location);
	void Sync();

	void Truncate(idx_t new_size);
	void Trim(idx_t offset, idx_t length);

	FileHandle &GetFileHandle();

private:
	//! Throws if a write at [required_size] would exceed the mmap reserve. No-op otherwise.
	void EnsureMappedSize(idx_t required_size) const;

	static unique_ptr<DatabaseHandle> OpenMemoryMap(AttachedDatabase &db, const string &path,
	                                                const StorageManagerOptions &options, DatabaseOpenMode open_mode);
	static unique_ptr<DatabaseHandle> OpenFile(AttachedDatabase &db, const string &path,
	                                           const StorageManagerOptions &options, DatabaseOpenMode open_mode);

private:
	//! The file handle
	unique_ptr<FileHandle> handle;
	//! Memory-mapped view of the file in MAP mode. Mutually exclusive with `handle`.
	unique_ptr<MemoryMappedFile> mmap_handle;
};

} // namespace duckdb
