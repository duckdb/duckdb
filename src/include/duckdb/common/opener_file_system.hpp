//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/opener_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"

namespace duckdb {

// The OpenerFileSystem is wrapper for a file system that pushes an appropriate FileOpener into the various API calls
class OpenerFileSystem : public FileSystem {
public:
	virtual FileSystem &GetFileSystem() const = 0;
	virtual optional_ptr<FileOpener> GetOpener() const = 0;

	void VerifyNoOpener(optional_ptr<FileOpener> opener);
	void VerifyCanAccessDirectory(const string &path);
	void VerifyCanAccessFile(const string &path);
	void VerifyCanAccessExtension(const string &path, const FileOpenFlags &flags) {
		if (flags.OpenForWriting() && !flags.EnableExtensionInstall()) {
			throw PermissionException(
			    "File '%s' cannot be opened for writing since files ending with '.duckdb_extension' are reserved for "
			    "DuckDB extensions, and these can only be installed through the INSTALL command",
			    path);
		}
	}

	bool IsDuckDBExtensionName(const string &path) {
		return StringUtil::EndsWith(path, ".duckdb_extension");
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		GetFileSystem().Read(handle, buffer, nr_bytes, location);
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		throw InternalException("writing on the OpenerFileSystem is undefined");
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return GetFileSystem().Read(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return GetFileSystem().Write(handle, buffer, nr_bytes);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return GetFileSystem().GetFileSize(handle);
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return GetFileSystem().GetLastModifiedTime(handle);
	}
	string GetVersionTag(FileHandle &handle) override {
		return GetFileSystem().GetVersionTag(handle);
	}
	FileType GetFileType(FileHandle &handle) override {
		return GetFileSystem().GetFileType(handle);
	}
	FileMetadata Stats(FileHandle &handle) override {
		return GetFileSystem().Stats(handle);
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		GetFileSystem().Truncate(handle, new_size);
	}

	void FileSync(FileHandle &handle) override {
		GetFileSystem().FileSync(handle);
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return GetFileSystem().DirectoryExists(directory, GetOpener());
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return GetFileSystem().CreateDirectory(directory, GetOpener());
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return GetFileSystem().RemoveDirectory(directory, GetOpener());
	}

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(source);
		VerifyCanAccessFile(target);
		if (IsDuckDBExtensionName(target) && !IsDuckDBExtensionName(source)) {
			throw PermissionException(
			    "File '%s' cannot be moved to '%s', files ending with '.duckdb_extension' are reserved for DuckDB "
			    "extensions, and these can only be installed through the INSTALL command, or moved if both are "
			    "extensions'",
			    source, target);
		}
		GetFileSystem().MoveFile(source, target, GetOpener());
	}

	string GetHomeDirectory() override {
		return FileSystem::GetHomeDirectory(GetOpener());
	}

	string ExpandPath(const string &path) override {
		return FileSystem::ExpandPath(path, GetOpener());
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(filename);
		return GetFileSystem().FileExists(filename, GetOpener());
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return GetFileSystem().IsPipe(filename, GetOpener());
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(filename);
		GetFileSystem().RemoveFile(filename, GetOpener());
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(filename);
		return GetFileSystem().TryRemoveFile(filename, GetOpener());
	}

	void RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		for (const auto &filename : filenames) {
			VerifyCanAccessFile(filename);
		}
		GetFileSystem().RemoveFiles(filenames, GetOpener());
	}

	string PathSeparator(const string &path) override {
		return GetFileSystem().PathSeparator(path);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(path);
		return GetFileSystem().Glob(path, GetOpener().get());
	}

	std::string GetName() const override {
		return "OpenerFileSystem - " + GetFileSystem().GetName();
	}

	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		GetFileSystem().RegisterSubSystem(std::move(sub_fs));
	}

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		GetFileSystem().RegisterSubSystem(compression_type, std::move(fs));
	}

	void UnregisterSubSystem(const string &name) override {
		GetFileSystem().UnregisterSubSystem(name);
	}

	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override {
		return GetFileSystem().ExtractSubSystem(name);
	}

	void SetDisabledFileSystems(const vector<string> &names) override {
		GetFileSystem().SetDisabledFileSystems(names);
	}

	bool SubSystemIsDisabled(const string &name) override {
		return GetFileSystem().SubSystemIsDisabled(name);
	}

	bool IsDisabledForPath(const string &path) override {
		return GetFileSystem().IsDisabledForPath(path);
	}

	vector<string> ListSubSystems() override {
		return GetFileSystem().ListSubSystems();
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(file.path);
		if (IsDuckDBExtensionName(file.path)) {
			VerifyCanAccessExtension(file.path, flags);
		}
		return GetFileSystem().OpenFile(file, flags, GetOpener());
	}

	bool SupportsOpenFileExtended() const override {
		return true;
	}

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return GetFileSystem().ListFiles(directory, callback, GetOpener().get());
	}

	bool SupportsListFilesExtended() const override {
		return true;
	}

	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(path);
		return GetFileSystem().Glob(path, input, GetOpener());
	}

	bool SupportsGlobExtended() const override {
		return true;
	}

	string CanonicalizePath(const string &path_p, optional_ptr<FileOpener> opener = nullptr) override {
		VerifyNoOpener(opener);
		return GetFileSystem().CanonicalizePath(path_p, GetOpener());
	}

private:
	void VerifyCanAccessFileInternal(const string &path, FileType type);
};

} // namespace duckdb
