//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/caching_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class BlockHandle;
class BufferHandle;
class BufferManager;
class FileOpenFlags;
class FileSystem;
struct FileHandle;

struct CachingFileHandle;

//! CachingFileSystem is a read-only file system that closely resembles the FileSystem API.
//! Instead of reading into a designated buffer, it caches reads using the BufferManager,
//! it returns a BufferHandle and sets a pointer into it
class CachingFileSystem {
	friend struct CachingFileHandle;

	enum class CachedFileRangeOverlap { NONE, PARTIAL, FULL };

	//! Cached reads (immutable)
	struct CachedFileRange {
		CachedFileRange(shared_ptr<BlockHandle> block_handle, idx_t nr_bytes, idx_t location, time_t last_modified);
		~CachedFileRange();

		//! Gets the overlap between this file range and another
		CachedFileRangeOverlap GetOverlap(idx_t other_nr_bytes, idx_t other_location) const;
		CachedFileRangeOverlap GetOverlap(const CachedFileRange &other) const;

		//! Computes/verifies checksum over the buffer to ensure data was not modified (used for Verification only)
		void AddCheckSum();
		void VerifyCheckSum();

		shared_ptr<BlockHandle> block_handle;
		const idx_t nr_bytes;
		const idx_t location;
		const time_t last_modified;
#ifdef DEBUG
		hash_t checksum = 0;
#endif
	};

	//! Cached files
	struct CachedFile {
		explicit CachedFile(string path_p);

		//! Verifies that none of the ranges fully overlap
		void Verify() const;

		string path;
		map<idx_t, shared_ptr<CachedFileRange>> ranges;
		mutex lock;

		atomic<idx_t> file_size;
		atomic<time_t> last_modified;
		atomic<bool> can_seek;
		atomic<bool> on_disk_file;
	};

public:
	explicit CachingFileSystem(DatabaseInstance &db, bool enable);

public:
	void SetEnabled(bool enable);
	vector<CachedFileInformation> GetCachedFileInformation() const;

	DUCKDB_API static CachingFileSystem &Get(DatabaseInstance &db);
	DUCKDB_API static CachingFileSystem &Get(ClientContext &context);

	DUCKDB_API unique_ptr<CachingFileHandle> OpenFile(const string &path, FileOpenFlags flags);

private:
	//! Gets the cached file, or creates it if is not yet present
	CachedFile &GetOrCreateCachedFile(const string &path);

private:
	//! The FileSystem used to read/write files
	FileSystem &file_system;
	//! The BufferManager used to cache files
	BufferManager &buffer_manager;
	//! Whether or not file caching is enabled
	bool enable;
	//! Whether or not to check whether cached files are invalidated (due to modifying the file)
	bool check_cached_file_invalidation;
	//! Mapping from file path to cached file with cached ranges
	unordered_map<string, unique_ptr<CachedFile>> cached_files;
	//! Lock for accessing the cached files
	mutable mutex lock;
};

struct CachingFileHandle {
	using CachedFileRangeOverlap = CachingFileSystem::CachedFileRangeOverlap;
	using CachedFileRange = CachingFileSystem::CachedFileRange;
	using CachedFile = CachingFileSystem::CachedFile;

public:
	DUCKDB_API CachingFileHandle(CachingFileSystem &caching_file_system, CachedFile &cached_file, FileOpenFlags flags);

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
	//! Whether the range is still valid given the last modified time
	bool RangeIsValid(const CachedFileRange &range);
	//! Try to read from cache, return an invalid BufferHandle if it fails
	BufferHandle TryReadFromFileRange(CachedFileRange &file_range, data_ptr_t &buffer, idx_t nr_bytes, idx_t location);

private:
	//! The caching file system that was used to create this CachingFileHandle
	CachingFileSystem &caching_file_system;
	//! The associated CachedFile with cached ranges
	CachedFile &cached_file;
	//! Flags used to open the file
	FileOpenFlags flags;

	//! The underlying FileHandle (optional)
	unique_ptr<FileHandle> file_handle;
	//! Last modified time (if FileHandle is opened)
	time_t last_modified;
};

} // namespace duckdb
