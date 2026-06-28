//===----------------------------------------------------------------------===//
//                         DuckDB
//
// debug_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

// Forwards declaration.
class DebugFileSystem;

struct DebugFileHandle : public FileHandle {
	DebugFileHandle(DebugFileSystem &fs, unique_ptr<FileHandle> inner_p);
	void Close() override;
	bool CanSeek() override;
	idx_t GetProgress() override;
	FileCompressionType GetFileCompressionType() override;
	unique_ptr<FileHandle> inner;
};

//! Wraps a FileSystem and injects configurable latency on open/read/write.
class DebugFileSystem : public FileSystem {
public:
	explicit DebugFileSystem(unique_ptr<FileSystem> inner_fs);

	FileSystem &GetInnerFileSystem();

	void SetDelayMeanMs(double v);
	void SetDelayStddevMs(double v);

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	string GetName() const override;
	bool IsLocalFileSystem() const override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	FileMetadata Stats(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void FileSync(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool SupportsPositionalWrites(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;
	bool TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) override;
	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle, bool write) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) override;
	string PathSeparator(const string &path) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override;
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;
	void UnregisterSubSystem(const string &name) override;
	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;
	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;
	bool IsDisabledForPath(const string &path) override;
	vector<string> ListSubSystems() override;
	string GetHomeDirectory() override;
	string ExpandPath(const string &path) override;
	unique_ptr<MemoryMappedFile> MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                           const MMapOptions &options, optional_ptr<FileOpener> opener) override;

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

	bool SupportsOpenFileExtended() const override;

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;

	bool SupportsListFilesExtended() const override;

	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override;

	bool SupportsGlobExtended() const override;

	string CanonicalizePath(const string &path_p, optional_ptr<FileOpener> opener) override;

private:
	void ApplyDelay();

	unique_ptr<FileSystem> inner_fs;
	annotated_mutex random_engine_lock;
	RandomEngine random_engine DUCKDB_GUARDED_BY(random_engine_lock);
	double delay_mean_ms DUCKDB_GUARDED_BY(random_engine_lock) = 0.0;
	double delay_stddev_ms DUCKDB_GUARDED_BY(random_engine_lock) = 0.0;
};

} // namespace duckdb
