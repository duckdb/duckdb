//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/caching_file_system_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

class DatabaseInstance;
class QueryContext;

//! CachingFileHandleWrapper wraps CachingFileHandle to conform to FileHandle API
class CachingFileHandleWrapper : public FileHandle {
public:
	DUCKDB_API CachingFileHandleWrapper(CachingFileSystemWrapper &file_system, unique_ptr<CachingFileHandle> handle);
	DUCKDB_API ~CachingFileHandleWrapper() override;

	DUCKDB_API void Close() override;

	//! Get the underlying CachingFileHandle
	DUCKDB_API CachingFileHandle &GetCachingFileHandle();

private:
	unique_ptr<CachingFileHandle> caching_handle;
	//! Current position for non-seeking reads
	idx_t position;
	//! BufferHandle to keep cached data alive during reads
	BufferHandle current_buffer_handle;
};

//! CachingFileSystemWrapper wraps CachingFileSystem to conform to FileSystem API
class CachingFileSystemWrapper : public FileSystem {
public:
	DUCKDB_API CachingFileSystemWrapper(FileSystem &file_system, DatabaseInstance &db);
	DUCKDB_API ~CachingFileSystemWrapper() override;

	DUCKDB_API std::string GetName() const override;

	// FileSystem API implementation
	DUCKDB_API unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                           optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API unique_ptr<FileHandle> OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                          optional_ptr<FileOpener> opener = nullptr) override;

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
	DUCKDB_API void Truncate(FileHandle &handle, int64_t new_size) override;
	DUCKDB_API void FileSync(FileHandle &handle) override;

	DUCKDB_API bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

	DUCKDB_API bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	                         FileOpener *opener = nullptr) override;
	DUCKDB_API bool ListFilesExtended(const string &directory,
	                                 const std::function<void(OpenFileInfo &info)> &callback,
	                                 optional_ptr<FileOpener> opener) override;
	DUCKDB_API bool SupportsListFilesExtended() const override;

	DUCKDB_API void MoveFile(const string &source, const string &target,
	                        optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	DUCKDB_API bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	DUCKDB_API virtual string GetHomeDirectory() override;
	DUCKDB_API virtual string ExpandPath(const string &path) override;
	DUCKDB_API virtual string PathSeparator(const string &path) override;

	DUCKDB_API virtual vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	DUCKDB_API virtual void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override;
	DUCKDB_API virtual void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;
	DUCKDB_API virtual void UnregisterSubSystem(const string &name) override;
	DUCKDB_API virtual unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;
	DUCKDB_API virtual vector<string> ListSubSystems() override;
	DUCKDB_API virtual bool CanHandleFile(const string &fpath) override;

	DUCKDB_API virtual void Seek(FileHandle &handle, idx_t location) override;
	DUCKDB_API virtual void Reset(FileHandle &handle) override;
	DUCKDB_API virtual idx_t SeekPosition(FileHandle &handle) override;

	DUCKDB_API virtual bool IsManuallySet() override;
	DUCKDB_API virtual bool CanSeek() override;
	DUCKDB_API virtual bool OnDiskFile(FileHandle &handle) override;

	DUCKDB_API virtual unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
	                                                            bool write) override;

	DUCKDB_API virtual void SetDisabledFileSystems(const vector<string> &names) override;
	DUCKDB_API virtual bool SubSystemIsDisabled(const string &name) override;

protected:
	DUCKDB_API virtual unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                                          optional_ptr<FileOpener> opener) override;
	DUCKDB_API virtual bool SupportsOpenFileExtended() const override;

private:
	CachingFileSystem caching_file_system;
	FileSystem &underlying_file_system;
};

} // namespace duckdb

