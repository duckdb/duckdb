//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enable_shared_from_this_ipp.hpp"
#include "duckdb/main/context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/storage/caching_mode.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;
class CachingFileSystemWrapper;
struct CachingFileHandle;

//! CachingFileHandleWrapper wraps CachingFileHandle to conform to FileHandle API.
class CachingFileHandleWrapper : public FileHandle {
	friend class CachingFileSystemWrapper;

public:
	DUCKDB_API CachingFileHandleWrapper(shared_ptr<CachingFileSystemWrapper> file_system,
	                                    unique_ptr<CachingFileHandle> handle, const FileOpenFlags &flags);

	DUCKDB_API ~CachingFileHandleWrapper() override;

	DUCKDB_API void Close() override;
	DUCKDB_API bool CanSeek() override;

private:
	// CachingFileSystem is not kept within VFS as other filesystems, so sometimes it's necessary to pin it inside of
	// file handle and ensure it's valid.
	shared_ptr<CachingFileSystemWrapper> caching_file_system;

	unique_ptr<CachingFileHandle> caching_handle;
};

//! [CachingFileSystemWrapper] is an adapter class, which wraps [CachingFileSystem] to conform to BaseFileSystem API.
//! Different from [CachingFileSystem], which owns cache content and returns a [BufferHandle] to achieve zero-copy on
//! read, the wrapper class always copies requested bytes into the provided address.
//!
//! NOTICE: Currently only read and seek operations are supported, write operations are disabled.
class CachingFileSystemWrapper : public BaseFileSystem, public enable_shared_from_this<CachingFileSystemWrapper> {
public:
	DUCKDB_API CachingFileSystemWrapper(FileSystem &file_system, DatabaseInstance &db,
	                                    CachingMode mode = CachingMode::CACHE_REMOTE_ONLY);
	DUCKDB_API ~CachingFileSystemWrapper() override;

	DUCKDB_API std::string GetName() const override;

	DUCKDB_API unique_ptr<FileHandle> OpenFile(Context &context, const string &path, FileOpenFlags flags) override;
	DUCKDB_API unique_ptr<FileHandle> OpenFile(Context &context, const OpenFileInfo &path, FileOpenFlags flags);

	DUCKDB_API void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	DUCKDB_API void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	DUCKDB_API int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	DUCKDB_API int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	DUCKDB_API bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;

	DUCKDB_API int64_t GetFileSize(FileHandle &handle) override;
	DUCKDB_API timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	DUCKDB_API string GetVersionTag(FileHandle &handle) override;
	DUCKDB_API FileType GetFileType(FileHandle &handle) override;
	DUCKDB_API FileMetadata Stats(FileHandle &handle) override;
	DUCKDB_API optional<FileMetadata> GetStatsIfExists(Context &context, const OpenFileInfo &file) override;
	DUCKDB_API void Truncate(FileHandle &handle, int64_t new_size) override;
	DUCKDB_API void FileSync(FileHandle &handle) override;

	DUCKDB_API bool DirectoryExists(Context &context, const string &directory) override;
	DUCKDB_API void CreateDirectory(Context &context, const string &directory) override;
	DUCKDB_API bool CreateDirectoryExtended(Context &context, const string &directory,
	                                        const CreateDirectoryOptions &options) override;
	DUCKDB_API void CreateDirectoriesRecursive(Context &context, const string &path) override;
	DUCKDB_API void RemoveDirectory(Context &context, const string &directory) override;
	DUCKDB_API bool RemoveDirectoryExtended(Context &context, const string &directory,
	                                        const RemoveDirectoryOptions &options) override;

	DUCKDB_API bool ListFiles(Context &context, const string &directory,
	                          const std::function<void(const string &, bool)> &callback) override;

	DUCKDB_API void MoveFile(Context &context, const string &source, const string &target) override;
	DUCKDB_API bool FileExists(Context &context, const string &filename) override;
	DUCKDB_API bool IsPipe(Context &context, const string &filename) override;
	DUCKDB_API void RemoveFile(Context &context, const string &filename) override;
	DUCKDB_API bool TryRemoveFile(Context &context, const string &filename) override;

	DUCKDB_API string GetHomeDirectory() override;
	DUCKDB_API string ExpandPath(const string &path) override;
	DUCKDB_API string PathSeparator(const string &path) override;

	DUCKDB_API vector<OpenFileInfo> Glob(Context &context, const string &path) override;

	DUCKDB_API bool CanHandleFile(const string &fpath) override;

	DUCKDB_API void Seek(FileHandle &handle, idx_t location) override;
	DUCKDB_API void Reset(FileHandle &handle) override;
	DUCKDB_API idx_t SeekPosition(FileHandle &handle) override;

	DUCKDB_API bool IsManuallySet() override;
	DUCKDB_API bool OnDiskFile(FileHandle &handle) override;

	DUCKDB_API unique_ptr<FileHandle> OpenCompressedFile(Context &context, unique_ptr<FileHandle> handle,
	                                                     bool write) override;

protected:
	DUCKDB_API unique_ptr<FileHandle> OpenFileExtended(Context &context, const OpenFileInfo &path,
	                                                   FileOpenFlags flags) override;
	DUCKDB_API bool SupportsOpenFileExtended() const override;
	DUCKDB_API bool ListFilesExtended(Context &context, const string &directory,
	                                  const std::function<void(OpenFileInfo &info)> &callback) override;
	DUCKDB_API bool SupportsListFilesExtended() const override;

private:
	bool ShouldUseCache(const string &path) const;

	// Return an optional caching file handle, if certain filepath is cached.
	CachingFileHandle *GetCachingHandleIfPossible(FileHandle &handle);

	//! The bound file system the files are opened with; it passes its own context
	FileSystem &underlying_file_system;
	CachingFileSystem caching_file_system;
	CachingMode caching_mode;
};

} // namespace duckdb
