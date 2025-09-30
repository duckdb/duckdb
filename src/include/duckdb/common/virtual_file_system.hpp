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

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	void RegisterSubSystem(unique_ptr<FileSystem> fs) override;

	void UnregisterSubSystem(const string &name) override;

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override;

	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override;

	vector<string> ListSubSystems() override;

	std::string GetName() const override;

	void SetDisabledFileSystems(const vector<string> &names) override;
	bool SubSystemIsDisabled(const string &name) override;

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
	FileSystem &FindFileSystem(const string &path, optional_ptr<ClientContext> client_context);
	FileSystem &FindFileSystem(const string &path);
	FileSystem &FindFileSystemInternal(const string &path, bool allow_autoloading_file_system = false);

private:
	vector<unique_ptr<FileSystem>> sub_systems;
	map<FileCompressionType, unique_ptr<FileSystem>> compressed_fs;
	const unique_ptr<FileSystem> default_fs;
	unordered_set<string> disabled_file_systems;
};

class AutoloadingFileSystem : public FileSystem {
public:
	bool CanHandleFile(const string &path) override {
		string required_extension = "";
		for (const auto &entry : EXTENSION_FILE_PREFIXES) {
			if (StringUtil::StartsWith(path, entry.name)) {
				required_extension = entry.extension;
				return true;
			}
		}
		// success! glob again
		return false;
	}
	std::string GetName() const override {
		return "AutoloadingBaseFileSystem";
	}

	bool Autoload(const string &path, optional_ptr<ClientContext> context, string &required_extension) {
		for (const auto &entry : EXTENSION_FILE_PREFIXES) {
			if (StringUtil::StartsWith(path, entry.name)) {
				required_extension = entry.extension;
			}
		}
		if (!required_extension.empty() && context && !context->db->ExtensionIsLoaded(required_extension)) {
			auto &dbconfig = DBConfig::GetConfig(*context);
			if (!ExtensionHelper::CanAutoloadExtension(required_extension) ||
			    !dbconfig.options.autoload_known_extensions) {
				auto error_message = "File " + path + " requires the extension " + required_extension + " to be loaded";
				error_message =
				    ExtensionHelper::AddExtensionInstallHintToErrorMsg(*context, error_message, required_extension);
				throw MissingExtensionException(error_message);
			}
			// an extension is required to read this file, but it is not loaded - try to load it
			ExtensionHelper::AutoLoadExtension(*context, required_extension);
		}
		return true;
	};
	bool IsAutoloadingFileSystem() override {
		return true;
	}
};

} // namespace duckdb
