//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/base_file_system.hpp"

namespace duckdb {
class MultiFileList;

//! A file system bound to a context: it wraps a BaseFileSystem and pushes the FileOpener of that context into every
//! call after checking that the access is allowed. It is not a BaseFileSystem: code that obtains it through
//! FileSystem::GetFileSystem(context) cannot call the opener-taking BaseFileSystem methods by accident.
class FileSystem {
public:
	virtual ~FileSystem() = default;

	virtual BaseFileSystem &GetFileSystem() const = 0;
	virtual optional_ptr<FileOpener> GetOpener() const = 0;

public:
	//! The file system of a client, of a database, or the local file system of a database
	DUCKDB_API static FileSystem &GetFileSystem(ClientContext &context);
	DUCKDB_API static FileSystem &GetFileSystem(DatabaseInstance &db);
	DUCKDB_API static FileSystem &GetLocal(DatabaseInstance &db);
	DUCKDB_API static FileSystem &Get(AttachedDatabase &db);

public:
	//! Static helpers that do not depend on a file system instance
	//! Whether or not a file is remote or local, based only on file path
	DUCKDB_API static bool IsRemoteFile(const string &path);
	DUCKDB_API static bool IsRemoteFile(const string &path, string &extension);
	//! Whether there is a glob in the string
	DUCKDB_API static bool HasGlob(const string &str);
	DUCKDB_API static bool IsDirectory(const OpenFileInfo &info);
	//! Sets the working directory
	DUCKDB_API static void SetWorkingDirectory(const string &path);
	//! Gets the working directory
	DUCKDB_API static string GetWorkingDirectory();
	//! Gets the users home directory
	DUCKDB_API static string GetHomeDirectory(optional_ptr<FileOpener> opener);
	//! Expands a given path, including e.g. expanding the home directory of the user
	DUCKDB_API static string ExpandPath(const string &path, optional_ptr<FileOpener> opener);
	//! Returns the system-available memory in bytes. Returns an invalid index if the system function fails.
	DUCKDB_API static optional_idx GetAvailableMemory();
	//! Returns the space available on the disk. Returns an invalid index if the information was not available.
	DUCKDB_API static optional_idx GetAvailableDiskSpace(const string &path);
	//! Returns the soft limit on open file descriptors, or an invalid index when it is unlimited or unknown.
	DUCKDB_API static optional_idx GetFileDescriptorLimit();
	//! Returns the share of the file descriptor limit that one consumer of descriptors may hold open.
	DUCKDB_API static idx_t GetFileDescriptorShare(idx_t file_descriptor_limit);
	//! Returns the value of an environment variable - or the empty string if it is not set
	DUCKDB_API static string GetEnvVariable(const string &name);

public:
	DUCKDB_API void VerifyCanAccessDirectory(const string &path);
	DUCKDB_API void VerifyCanAccessFile(const string &path);
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
		auto name = StringUtil::Lower(GetFileSystem().ExtractName(CanonicalizePath(path)));
		return StringUtil::EndsWith(name, ".duckdb_extension") || StringUtil::Contains(name, ".duckdb_extension.");
	}

	//! Handle-level operations
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
		GetFileSystem().Read(handle, buffer, nr_bytes, location);
	}
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
		return GetFileSystem().Read(handle, buffer, nr_bytes);
	}
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
		GetFileSystem().Write(handle, buffer, nr_bytes, location);
	}
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
		return GetFileSystem().Write(handle, buffer, nr_bytes);
	}
	int64_t GetFileSize(FileHandle &handle) {
		return GetFileSystem().GetFileSize(handle);
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) {
		return GetFileSystem().GetLastModifiedTime(handle);
	}
	string GetVersionTag(FileHandle &handle) {
		return GetFileSystem().GetVersionTag(handle);
	}
	FileType GetFileType(FileHandle &handle) {
		return GetFileSystem().GetFileType(handle);
	}
	FileMetadata Stats(FileHandle &handle) {
		return GetFileSystem().Stats(handle);
	}
	void Truncate(FileHandle &handle, int64_t new_size) {
		GetFileSystem().Truncate(handle, new_size);
	}
	void FileSync(FileHandle &handle) {
		GetFileSystem().FileSync(handle);
	}

	//! Path-level operations: the access is verified, then forwarded together with the opener
	DUCKDB_API unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags);
	DUCKDB_API unique_ptr<FileHandle> OpenFile(const OpenFileInfo &file, const FileOpenFlags &flags);
	DUCKDB_API unique_ptr<MemoryMappedFile> MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
	                                                      const MMapOptions &options);

	DUCKDB_API bool DirectoryExists(const string &directory);
	DUCKDB_API void CreateDirectory(const string &directory);
	DUCKDB_API bool CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options);
	DUCKDB_API void CreateDirectoriesRecursive(const string &path);
	DUCKDB_API void RemoveDirectory(const string &directory);
	DUCKDB_API bool RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options);
	DUCKDB_API void MoveFile(const string &source, const string &target);
	DUCKDB_API bool FileExists(const string &filename);
	DUCKDB_API optional<FileMetadata> GetStatsIfExists(const OpenFileInfo &file);
	DUCKDB_API bool IsPipe(const string &filename);
	DUCKDB_API void RemoveFile(const string &filename);
	DUCKDB_API bool TryRemoveFile(const string &filename);
	DUCKDB_API void RemoveFiles(const vector<string> &filenames);
	DUCKDB_API bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback);
	DUCKDB_API bool ListFiles(const string &directory, const std::function<void(OpenFileInfo &info)> &callback);
	DUCKDB_API vector<OpenFileInfo> Glob(const string &path);
	DUCKDB_API unique_ptr<MultiFileList> Glob(const string &path, const FileGlobInput &input);
	DUCKDB_API unique_ptr<MultiFileList> GlobFileList(const string &path,
	                                                  const FileGlobInput &input = FileGlobOptions::DISALLOW_EMPTY);
	DUCKDB_API vector<OpenFileInfo> GlobFiles(const string &pattern,
	                                          const FileGlobInput &input = FileGlobOptions::DISALLOW_EMPTY);
	DUCKDB_API string CanonicalizePath(const string &path);
	DUCKDB_API string GetHomeDirectory();
	DUCKDB_API string ExpandPath(const string &path);

	//! Path helpers that do not touch the file system
	string PathSeparator(const string &path) {
		return GetFileSystem().PathSeparator(path);
	}
	bool IsPathAbsolute(const string &path) {
		return GetFileSystem().IsPathAbsolute(path);
	}
	string JoinPath(const string &a, const string &path) {
		return GetFileSystem().JoinPath(a, path);
	}
	template <typename... ARGS>
	string JoinPath(const string &a, const string &b, ARGS... args) {
		return GetFileSystem().JoinPath(a, b, args...);
	}
	string ConvertSeparators(const string &path) {
		return GetFileSystem().ConvertSeparators(path);
	}
	string ExtractBaseName(const string &path) {
		return GetFileSystem().ExtractBaseName(path);
	}
	string ExtractExtension(const string &path) {
		return GetFileSystem().ExtractExtension(path);
	}
	string ExtractName(const string &path) {
		return GetFileSystem().ExtractName(path);
	}

	//! Sub-file system management
	std::string GetName() const {
		return "FileSystem - " + GetFileSystem().GetName();
	}
	void RegisterSubSystem(unique_ptr<BaseFileSystem> sub_fs) {
		GetFileSystem().RegisterSubSystem(std::move(sub_fs));
	}
	void RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs);
	void UnregisterSubSystem(const string &name) {
		GetFileSystem().UnregisterSubSystem(name);
	}
	unique_ptr<BaseFileSystem> ExtractSubSystem(const string &name) {
		return GetFileSystem().ExtractSubSystem(name);
	}
	void SetDisabledFileSystems(const vector<string> &names) {
		GetFileSystem().SetDisabledFileSystems(names);
	}
	vector<string> GetDisabledFileSystems() {
		return GetFileSystem().GetDisabledFileSystems();
	}
	bool SubSystemIsDisabled(const string &name) {
		return GetFileSystem().SubSystemIsDisabled(name);
	}
	bool IsDisabledForPath(const string &path) {
		return GetFileSystem().IsDisabledForPath(path);
	}
	vector<string> ListSubSystems() {
		return GetFileSystem().ListSubSystems();
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

private:
	void VerifyCanAccessFileInternal(const string &path, FileType type);
};

//! Binds a BaseFileSystem to an (optional) FileOpener
class BoundFileSystem : public FileSystem {
public:
	BoundFileSystem(BaseFileSystem &file_system_p, optional_ptr<FileOpener> opener_p)
	    : file_system(file_system_p), opener(opener_p) {
	}

	BaseFileSystem &GetFileSystem() const override {
		return file_system;
	}
	optional_ptr<FileOpener> GetOpener() const override {
		return opener;
	}

private:
	BaseFileSystem &file_system;
	optional_ptr<FileOpener> opener;
};

} // namespace duckdb
