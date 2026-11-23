//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/opener_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"

namespace duckdb {
class MultiFileList;

// The OpenerFileSystem is wrapper for a file system that pushes an appropriate FileOpener into the various API calls
class OpenerFileSystem : public FileSystem {
public:
	virtual VirtualFileSystem &GetFileSystem() const = 0;
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

	//! Reserved for DuckDB's extension trust domain: the loadable binary and every file that decides whether it may
	//! be loaded (install provenance, trusted repositories). These can only be written through INSTALL / CREATE
	//! EXTENSION REPOSITORY, which pass FILE_FLAGS_ENABLE_EXTENSION_INSTALL.
	//! To add a file to the trust domain, give it a ".duckdb_extension." segment, e.g.
	//! "<name>.duckdb_extension.info" or "<name>.duckdb_extension.repo.json". Matching is on the file name only, so a
	//! directory that happens to contain the marker does not reserve the files inside it
	bool IsDuckDBExtensionName(const string &path) {
		// match the canonical name case-insensitively: on the default macOS and Windows file systems a
		// differently-cased name refers to the same file
		auto name = StringUtil::Lower(FileSystem::ExtractName(CanonicalizePath(path)));
		return StringUtil::EndsWith(name, ".duckdb_extension") || StringUtil::Contains(name, ".duckdb_extension.");
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		handle.file_system.Read(handle, buffer, nr_bytes, location);
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		throw InternalException("writing on the OpenerFileSystem is undefined");
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return handle.file_system.Read(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return handle.file_system.Write(handle, buffer, nr_bytes);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return handle.file_system.GetFileSize(handle);
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return handle.file_system.GetLastModifiedTime(handle);
	}
	string GetVersionTag(FileHandle &handle) override {
		return handle.file_system.GetVersionTag(handle);
	}
	FileType GetFileType(FileHandle &handle) override {
		return handle.file_system.GetFileType(handle);
	}
	FileMetadata Stats(FileHandle &handle) override {
		return handle.file_system.Stats(handle);
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		handle.file_system.Truncate(handle, new_size);
	}

	void FileSync(FileHandle &handle) override {
		handle.file_system.FileSync(handle);
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return FindFileSystem(directory).DirectoryExists(directory, GetOpener());
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		CreateDirectoryExtended(directory, {CreateDirectoryMode::SINGLE}, opener);
	}
	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override {
		if (options.mode == CreateDirectoryMode::RECURSIVE) {
			return FileSystem::CreateDirectoryExtended(directory, options, opener);
		}
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return FindFileSystem(directory).CreateDirectoryExtended(directory, options, GetOpener());
	}
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override {
		CreateDirectoryExtended(path, {CreateDirectoryMode::RECURSIVE}, opener);
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		RemoveDirectoryExtended(directory, {RemoveDirectoryMode::RECURSIVE}, opener);
	}
	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
	                             optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return FindFileSystem(directory).RemoveDirectoryExtended(directory, options, GetOpener());
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
		FindFileSystem(source).MoveFile(source, target, GetOpener());
	}

	bool DirectoryExists(const string &directory) {
		return DirectoryExists(directory, nullptr);
	}
	void CreateDirectory(const string &directory) {
		CreateDirectory(directory, nullptr);
	}
	bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options) {
		return CreateDirectoryExtended(directory, options, nullptr);
	}
	void RemoveDirectory(const string &directory) {
		RemoveDirectory(directory, nullptr);
	}
	bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options) {
		return RemoveDirectoryExtended(directory, options, nullptr);
	}
	void MoveFile(const string &source, const string &target) {
		MoveFile(source, target, nullptr);
	}
	optional<FileMetadata> GetStatsIfExists(const OpenFileInfo &file) {
		return GetStatsIfExists(file, nullptr);
	}
	bool FileExists(const string &filename) {
		return FileExists(filename, nullptr);
	}
	bool IsPipe(const string &filename) {
		return IsPipe(filename, nullptr);
	}
	void RemoveFile(const string &filename) {
		RemoveFile(filename, nullptr);
	}
	bool TryRemoveFile(const string &filename) {
		return TryRemoveFile(filename, nullptr);
	}
	void RemoveFiles(const vector<string> &filenames) {
		RemoveFiles(filenames, nullptr);
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
		return FindFileSystem(filename).FileExists(filename, GetOpener());
	}

	optional<FileMetadata> GetStatsIfExists(const OpenFileInfo &file, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(file.path);
		return FindFileSystem(file.path).GetStatsIfExists(file, GetOpener());
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return FindFileSystem(filename).IsPipe(filename, GetOpener());
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(filename);
		FindFileSystem(filename).RemoveFile(filename, GetOpener());
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(filename);
		return FindFileSystem(filename).TryRemoveFile(filename, GetOpener());
	}

	void RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		reference_map_t<FileSystem, vector<string>> files_by_fs;
		for (const auto &filename : filenames) {
			VerifyCanAccessFile(filename);
			files_by_fs[FindFileSystem(filename)].push_back(filename);
		}
		for (auto &entry : files_by_fs) {
			entry.first.get().RemoveFiles(entry.second, GetOpener());
		}
	}

	string PathSeparator(const string &path) override {
		return GetFileSystem().FindFileSystem(path, nullptr).PathSeparator(path);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		VerifyNoOpener(opener);
		VerifyCanAccessFile(path);
		return FindFileSystem(path).Glob(path, GetOpener().get());
	}

	std::string GetName() const override {
		return "OpenerFileSystem - " + GetFileSystem().GetName();
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

public:
	unique_ptr<MemoryMappedFile> MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                           const MMapOptions &options,
	                                           optional_ptr<FileOpener> opener = nullptr) override;

protected:
	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		VerifyCanAccessDirectory(directory);
		return FindFileSystem(directory).ListFiles(directory, callback, GetOpener());
	}

	bool SupportsListFilesExtended() const override {
		return true;
	}

	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override;

	bool SupportsGlobExtended() const override {
		return true;
	}

	string CanonicalizePath(const string &path_p, optional_ptr<FileOpener> opener = nullptr) override {
		VerifyNoOpener(opener);
		return FindFileSystem(path_p).CanonicalizePath(path_p, GetOpener());
	}

private:
	//! The file system that handles the path
	FileSystem &FindFileSystem(const string &path) {
		return GetFileSystem().FindFileSystem(path, GetOpener());
	}
	void VerifyCanAccessFileInternal(const string &path, FileType type);
};

} // namespace duckdb
