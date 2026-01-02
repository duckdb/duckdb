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

// Forward declaration.
class DatabaseInstance;
class ClientContext;
class QueryContext;
class CachingFileSystemWrapper;
struct CachingFileHandle;

//! Caching mode for CachingFileSystemWrapper.
//! By default only remote files will be cached, but it's also allowed to cache local for direct IO use case.
enum class CachingMode : uint8_t {
	// Cache all files.
	ALWAYS_CACHE = 0,
	// Only cache remote files, bypass cache for local files.
	CACHE_REMOTE_ONLY = 1,
};

//! CachingFileHandleWrapper wraps CachingFileHandle to conform to FileHandle API.
class CachingFileHandleWrapper : public FileHandle {
	friend class CachingFileSystemWrapper;

public:
	CachingFileHandleWrapper(CachingFileSystemWrapper &file_system, unique_ptr<CachingFileHandle> handle,
	                         FileOpenFlags flags);
	~CachingFileHandleWrapper() override;

	void Close() override;

private:
	unique_ptr<CachingFileHandle> caching_handle;
};

//! [CachingFileSystemWrapper] is an adapter class, which wraps [CachingFileSystem] to conform to FileSystem API.
//! Different from [CachingFileSystem], which owns cache content and returns a [BufferHandle] to achieve zero-copy on
//! read, the wrapper class always copies requested byted into the provided address.
//!
//! NOTICE: Currently only read and seek operations are supported, write operations are disabled.
class CachingFileSystemWrapper : public FileSystem {
public:
	CachingFileSystemWrapper(FileSystem &file_system, DatabaseInstance &db,
	                         CachingMode mode = CachingMode::CACHE_REMOTE_ONLY);
	~CachingFileSystemWrapper() override;

	static CachingFileSystemWrapper Get(ClientContext &context, CachingMode mode = CachingMode::CACHE_REMOTE_ONLY);

	std::string GetName() const override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	unique_ptr<FileHandle> OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr);

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	FileMetadata Stats(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void FileSync(FileHandle &handle) override;

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	string GetHomeDirectory() override;
	string ExpandPath(const string &path) override;
	string PathSeparator(const string &path) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override;
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;
	void UnregisterSubSystem(const string &name) override;
	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;
	vector<string> ListSubSystems() override;
	bool CanHandleFile(const string &fpath) override;

	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;

	bool IsManuallySet() override;
	bool CanSeek() override;
	bool CanSeek(const string &filepath) override;
	bool OnDiskFile(FileHandle &handle) override;

	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle, bool write) override;

	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;
	bool IsDisabledForPath(const string &path) override;

	bool IsWrapperFileSystem() const override {
		return true;
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override;
	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override;

private:
	bool ShouldUseCache(const string &path) const;

	// Return an optional caching file handle, if certain filepath is cached.
	CachingFileHandle *GetCachingHandleIfPossible(FileHandle &handle);

	CachingFileSystem caching_file_system;
	FileSystem &underlying_file_system;
	CachingMode caching_mode;
};

} // namespace duckdb
