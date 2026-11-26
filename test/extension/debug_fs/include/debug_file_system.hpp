//===----------------------------------------------------------------------===//
//                         DuckDB
//
// debug_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/base_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/random_engine.hpp"

#include <string_view>

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

// Forwards declaration.
class DebugFileSystem;

struct DebugFileHandle : public FileHandle {
	DebugFileHandle(DebugFileSystem &fs, string path, unique_ptr<FileHandle> inner_p);
	void Close() override;
	bool CanSeek() override;
	idx_t GetProgress() override;
	FileCompressionType GetFileCompressionType() override;
	unique_ptr<FileHandle> inner;
};

//! Handles "debugfs://<local path>": forwards to the local file system and injects configurable latency on
//! open/read/write.
class DebugFileSystem : public BaseFileSystem {
public:
	explicit DebugFileSystem(DatabaseInstance &db);

	static constexpr std::string_view PATH_PREFIX = "debugfs://";

	void SetDelayMeanMs(double v);
	void SetDelayStddevMs(double v);
	void SetRandomSeed(optional_idx seed);
	bool RandomEngineInitialized();

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	string GetName() const override;
	bool IsLocalFileSystem() const override;
	bool CanHandleFile(const string &fpath) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	FileMetadata Stats(FileHandle &handle) override;
	optional<FileMetadata> GetStatsIfExists(const OpenFileInfo &file, optional_ptr<FileOpener> opener) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void FileSync(FileHandle &handle) override;
	void AbortFileWrite(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	FileWriteMode GetWriteMode(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;
	bool TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) override;
	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle, bool write) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override;
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override;
	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) override;
	string PathSeparator(const string &path) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
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
	// Random engine is initialized lazily on first IO operation, so users could set the random seed for reproduction.
	void ApplyDelay();
	void EnsureRandomEngineInitialized();
	//! The local path of a "debugfs://" path
	static string ToInner(const string &path);
	static OpenFileInfo ToInner(const OpenFileInfo &file);
	static string FromInner(const string &path);

	unique_ptr<BaseFileSystem> inner_fs;
	DatabaseInstance &db;
	annotated_mutex random_engine_lock;
	unique_ptr<RandomEngine> random_engine DUCKDB_GUARDED_BY(random_engine_lock);
	optional_idx random_seed DUCKDB_GUARDED_BY(random_engine_lock);
	double delay_mean_ms DUCKDB_GUARDED_BY(random_engine_lock) = 0.0;
	double delay_stddev_ms DUCKDB_GUARDED_BY(random_engine_lock) = 0.0;
};

} // namespace duckdb
