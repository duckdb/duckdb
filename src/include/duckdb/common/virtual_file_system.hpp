//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/virtual_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

// bunch of wrappers to allow registering protocol handlers
class VirtualFileSystem : public FileSystem {
public:
	VirtualFileSystem();

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;

	void Truncate(FileHandle &handle, int64_t new_size) override;

	void FileSync(FileHandle &handle) override;

	// need to look up correct fs for this
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override;

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override;

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	void RegisterSubSystem(unique_ptr<FileSystem> fs) override;

	void UnregisterSubSystem(const string &name) override;

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;

	vector<string> ListSubSystems() override;

	std::string GetName() const override;

	void SetDisabledFileSystems(const vector<string> &names) override;

	string PathSeparator(const string &path) override;

private:
	FileSystem &FindFileSystem(const string &path);
	FileSystem &FindFileSystemInternal(const string &path);

private:
	vector<unique_ptr<FileSystem>> sub_systems;
	map<FileCompressionType, unique_ptr<FileSystem>> compressed_fs;
	const unique_ptr<FileSystem> default_fs;
	unordered_set<string> disabled_file_systems;
};

} // namespace duckdb
