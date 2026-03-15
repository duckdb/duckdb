//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/caching_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/file_buffer_handle_group.hpp"

namespace duckdb {

class BufferHandle;
class ClientContext;
class DatabaseInstance;
class FileOpenFlags;
class FileSystem;
struct FileHandle;
class QueryContext;
class CachingFileSystem;

struct CachingFileHandle {
public:
	using CachedFile = ExternalFileCache::CachedFile;

public:
	DUCKDB_API CachingFileHandle(QueryContext context, CachingFileSystem &caching_file_system, const OpenFileInfo &path,
	                             FileOpenFlags flags, optional_ptr<FileOpener> opener, CachedFile &cached_file);
	DUCKDB_API ~CachingFileHandle();

public:
	//! Get the underlying FileHandle
	DUCKDB_API FileHandle &GetFileHandle();
	//! Read nr_bytes from the file (or cache) at location.
	//! Returns a FileBufferHandleGroup that keeps the data pinned in memory.
	DUCKDB_API FileBufferHandleGroup Read(idx_t nr_bytes, idx_t location);
	//! Read (non-seeking) nr bytes from the file (or cache), sets nr_bytes to actually read bytes
	DUCKDB_API FileBufferHandleGroup Read(idx_t &nr_bytes);
	//! Get some properties of the file
	DUCKDB_API string GetPath() const;
	DUCKDB_API idx_t GetFileSize();
	DUCKDB_API timestamp_t GetLastModifiedTime();
	DUCKDB_API string GetVersionTag();
	DUCKDB_API bool Validate() const;
	DUCKDB_API bool CanSeek();
	DUCKDB_API bool IsRemoteFile() const;
	DUCKDB_API bool OnDiskFile();
	DUCKDB_API idx_t SeekPosition();
	DUCKDB_API void Seek(idx_t location);

private:
	QueryContext context;

	//! The client caching file system that was used to create this CachingFileHandle
	CachingFileSystem &caching_file_system;
	//! The DB external file cache
	ExternalFileCache &external_file_cache;
	//! For opening the file (possibly with extra info)
	OpenFileInfo path;
	//! Flags used to open the file
	FileOpenFlags flags;
	//! File opener, which contains file open context.
	optional_ptr<FileOpener> opener;
	//! Cache validation mode for this file
	CacheValidationMode validate;
	//! The associated CachedFile with cached blocks
	CachedFile &cached_file;

	//! The underlying FileHandle (optional)
	unique_ptr<FileHandle> file_handle;
	//! Last modified time and version tag (if FileHandle is opened)
	timestamp_t last_modified;
	string version_tag;

	//! Current position (if non-seeking reads)
	idx_t position;
};

//! CachingFileSystem is a read-only file system that closely resembles the FileSystem API.
//! Instead of reading into a designated buffer, it caches reads using the BufferManager,
//! it returns a BufferHandle and sets a pointer into it
class CachingFileSystem {
private:
	friend struct CachingFileHandle;

public:
	// Notice, the provided [file_system] should be a raw, non-caching filesystem.
	DUCKDB_API CachingFileSystem(FileSystem &file_system, DatabaseInstance &db);
	DUCKDB_API ~CachingFileSystem();

public:
	DUCKDB_API static CachingFileSystem Get(ClientContext &context);

	DUCKDB_API unique_ptr<CachingFileHandle> OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                                  optional_ptr<FileOpener> opener = nullptr);
	DUCKDB_API unique_ptr<CachingFileHandle> OpenFile(QueryContext context, const OpenFileInfo &path,
	                                                  FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr);

private:
	//! The Client FileSystem (needs to be client-specific so we can do, e.g., HTTPFS profiling)
	FileSystem &file_system;
	//! The DatabaseInstance.
	DatabaseInstance &db;
	//! The External File Cache that caches the files
	ExternalFileCache &external_file_cache;
};

} // namespace duckdb
