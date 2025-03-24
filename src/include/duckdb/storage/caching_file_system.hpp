//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/caching_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/external_file_cache.hpp"

namespace duckdb {

class ClientContext;
class BufferHandle;
class FileOpenFlags;
class FileSystem;
struct FileHandle;
class CachingFileSystem;

class CachingFileHandle {
	using CachedFileRangeOverlap = ExternalFileCache::CachedFileRangeOverlap;
	using CachedFileRange = ExternalFileCache::CachedFileRange;
	using CachedFile = ExternalFileCache::CachedFile;

public:
	DUCKDB_API CachingFileHandle(CachingFileSystem &caching_file_system, CachedFile &cached_file, FileOpenFlags flags);
	DUCKDB_API ~CachingFileHandle();

public:
	//! Get the underlying FileHandle
	DUCKDB_API FileHandle &GetFileHandle();
	//! Read nr_bytes from the file (or cache) at location. The pointer will be set to the requested range
	//! The buffer is guaranteed to stay in memory as long as the returned BufferHandle is in scope
	DUCKDB_API BufferHandle Read(data_ptr_t &buffer, idx_t nr_bytes, idx_t location);
	//! Get some properties of the file
	DUCKDB_API string GetPath() const;
	DUCKDB_API idx_t GetFileSize();
	DUCKDB_API time_t GetLastModifiedTime();
	DUCKDB_API bool CanSeek();
	DUCKDB_API bool OnDiskFile();

private:
	//! Get the version tag of the file (for checking cache invalidation)
	const string &GetVersionTag(const unique_ptr<StorageLockKey> &guard);
	//! Try to read from cache, return an invalid BufferHandle if it fails
	BufferHandle TryReadFromFileRange(const unique_ptr<StorageLockKey> &guard, CachedFileRange &file_range,
	                                  data_ptr_t &buffer, idx_t nr_bytes, idx_t location);
	//! Read from file and copy from cached buffers until the requested read is complete
	//! If actually_read is false, no reading happens, only the number of non-cached reads is counted and returned
	idx_t ReadAndCopyInterleaved(const vector<shared_ptr<CachedFileRange>> &overlapping_ranges,
	                             const shared_ptr<CachedFileRange> &new_file_range, data_ptr_t buffer, idx_t nr_bytes,
	                             idx_t location, bool actually_read);

private:
	//! The client caching file system that was used to create this CachingFileHandle
	CachingFileSystem &caching_file_system;
	//! The DB external file cache
	ExternalFileCache &external_file_cache;
	//! The associated CachedFile with cached ranges
	CachedFile &cached_file;
	//! Flags used to open the file
	FileOpenFlags flags;

	//! The underlying FileHandle (optional)
	unique_ptr<FileHandle> file_handle;
	//! Last modified time and version tag (if FileHandle is opened)
	time_t last_modified;
	string version_tag;
};

//! CachingFileSystem is a read-only file system that closely resembles the FileSystem API.
//! Instead of reading into a designated buffer, it caches reads using the BufferManager,
//! it returns a BufferHandle and sets a pointer into it
class CachingFileSystem {
	friend class CachingFileHandle;

public:
	DUCKDB_API CachingFileSystem(FileSystem &file_system, DatabaseInstance &db);
	DUCKDB_API ~CachingFileSystem();

public:
	DUCKDB_API static CachingFileSystem Get(ClientContext &context);

	DUCKDB_API unique_ptr<CachingFileHandle> OpenFile(const string &path, FileOpenFlags flags);

private:
	//! The Client FileSystem (needs to be client-specific so we can do, e.g., HTTPFS profiling)
	FileSystem &file_system;
	//! The External File Cache that caches the files
	ExternalFileCache &external_file_cache;
	//! Whether to validate cache entries
	bool validate;
};

} // namespace duckdb
