//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/virtual_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

// bunch of wrappers to allow registering protocol handlers
class VirtualFileSystem : public FileSystem {
public:
	VirtualFileSystem();
	explicit VirtualFileSystem(unique_ptr<FileSystem> &&inner_file_system);

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	FileMetadata Stats(FileHandle &handle) override;

	void Truncate(FileHandle &handle, int64_t new_size) override;

	void FileSync(FileHandle &handle) override;

	// need to look up correct fs for this
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override;

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	void RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	void RegisterSubSystem(unique_ptr<FileSystem> fs) override;

	void UnregisterSubSystem(const string &name) override;

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;

	void RegisterCompressionFilesystem(unique_ptr<FileSystem> fs) override;

	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;

	vector<string> ListSubSystems() override;

	std::string GetName() const override;

	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;
	bool IsDisabledForPath(const string &path) override;

	string PathSeparator(const string &path) override;

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override {
		return true;
	}

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;

	bool SupportsListFilesExtended() const override {
		return true;
	}

private:
	FileSystem &FindFileSystem(const string &path, optional_ptr<FileOpener> file_opener);
	FileSystem &FindFileSystem(const string &path, optional_ptr<DatabaseInstance> database_instance);
	FileSystem &FindFileSystem(const string &path);
	optional_ptr<FileSystem> FindFileSystemInternal(const string &path);
	// Return nullptr if no compression filesystem is found.
	FileSystem *FindCompressionFileSystem(FileCompressionType compression_type, const string &path);

private:
	vector<unique_ptr<FileSystem>> sub_systems;
	const unique_ptr<FileSystem> default_fs;
	unordered_set<string> disabled_file_systems;
	// Registered duckdb internal compression filesystem.
	unordered_map<FileCompressionType, unique_ptr<FileSystem>> compressed_fs;
	// Registered external compression filesystems (i.e., extensions).
	vector<unique_ptr<FileSystem>> external_compressed_fs;
};

} // namespace duckdb
