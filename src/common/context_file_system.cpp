#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/context_file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/memory_mapped_file.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void ContextFileSystem::RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs) {
	GetFileSystem().RegisterCompressionFilesystem(std::move(fs));
}

unique_ptr<FileHandle> ContextFileSystem::OpenFile(const string &path, FileOpenFlags flags) {
	return OpenFile(OpenFileInfo(path), flags);
}

unique_ptr<FileHandle> ContextFileSystem::OpenFile(const OpenFileInfo &file, const FileOpenFlags &flags) {
	VerifyCanAccessFile(file.path);
	if (IsDuckDBExtensionName(file.path)) {
		VerifyCanAccessExtension(file.path, flags);
	}
	return GetFileSystem().OpenFile(file, flags, GetOpener());
}

unique_ptr<MemoryMappedFile> ContextFileSystem::MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                              const MMapOptions &options) {
	VerifyCanAccessFile(path.path);
	if (IsDuckDBExtensionName(path.path)) {
		VerifyCanAccessExtension(path.path, flags);
	}
	return GetFileSystem().MemoryMapFile(path, flags, options, GetOpener());
}

bool ContextFileSystem::DirectoryExists(const string &directory) {
	VerifyCanAccessDirectory(directory);
	return GetFileSystem().DirectoryExists(directory, GetOpener());
}

void ContextFileSystem::CreateDirectory(const string &directory) {
	CreateDirectoryExtended(directory, {CreateDirectoryMode::SINGLE});
}

bool ContextFileSystem::CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options) {
	VerifyCanAccessDirectory(directory);
	return GetFileSystem().CreateDirectoryExtended(directory, options, GetOpener());
}

void ContextFileSystem::CreateDirectoriesRecursive(const string &path) {
	CreateDirectoryExtended(path, {CreateDirectoryMode::RECURSIVE});
}

void ContextFileSystem::RemoveDirectory(const string &directory) {
	RemoveDirectoryExtended(directory, {RemoveDirectoryMode::RECURSIVE});
}

bool ContextFileSystem::RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options) {
	VerifyCanAccessDirectory(directory);
	return GetFileSystem().RemoveDirectoryExtended(directory, options, GetOpener());
}

void ContextFileSystem::MoveFile(const string &source, const string &target) {
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

bool ContextFileSystem::FileExists(const string &filename) {
	VerifyCanAccessFile(filename);
	return GetFileSystem().FileExists(filename, GetOpener());
}

optional<FileMetadata> ContextFileSystem::GetStatsIfExists(const OpenFileInfo &file) {
	VerifyCanAccessFile(file.path);
	return GetFileSystem().GetStatsIfExists(file, GetOpener());
}

bool ContextFileSystem::IsPipe(const string &filename) {
	return GetFileSystem().IsPipe(filename, GetOpener());
}

void ContextFileSystem::RemoveFile(const string &filename) {
	VerifyCanAccessFile(filename);
	GetFileSystem().RemoveFile(filename, GetOpener());
}

bool ContextFileSystem::TryRemoveFile(const string &filename) {
	VerifyCanAccessFile(filename);
	return GetFileSystem().TryRemoveFile(filename, GetOpener());
}

void ContextFileSystem::RemoveFiles(const vector<string> &filenames) {
	for (const auto &filename : filenames) {
		VerifyCanAccessFile(filename);
	}
	GetFileSystem().RemoveFiles(filenames, GetOpener());
}

bool ContextFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	VerifyCanAccessDirectory(directory);
	return GetFileSystem().ListFiles(directory, callback, GetOpener().get());
}

bool ContextFileSystem::ListFiles(const string &directory, const std::function<void(OpenFileInfo &info)> &callback) {
	VerifyCanAccessDirectory(directory);
	return GetFileSystem().ListFiles(directory, callback, GetOpener());
}

vector<OpenFileInfo> ContextFileSystem::Glob(const string &path) {
	VerifyCanAccessFile(path);
	return GetFileSystem().Glob(path, GetOpener().get());
}

unique_ptr<MultiFileList> ContextFileSystem::Glob(const string &path, const FileGlobInput &input) {
	VerifyCanAccessFile(path);
	return GetFileSystem().Glob(path, input, GetOpener());
}

unique_ptr<MultiFileList> ContextFileSystem::GlobFileList(const string &pattern, const FileGlobInput &input) {
	auto result = Glob(pattern, input);
	if (result->IsEmpty()) {
		if (input.behavior == FileGlobOptions::FALLBACK_GLOB && !FileSystem::HasGlob(pattern)) {
			// if we have no glob in the pattern and we have an extension, we try to glob
			if (input.extension.empty()) {
				throw InternalException("FALLBACK_GLOB requires an extension to be specified");
			}
			string new_pattern = JoinPath(JoinPath(pattern, "**"), "*." + input.extension);
			result = GlobFileList(new_pattern, FileGlobOptions::ALLOW_EMPTY);
			if (!result->IsEmpty()) {
				// we found files by globbing the target as if it was a directory - return them
				return result;
			}
		}
		if (!input.AllowsEmpty()) {
			throw IOException("No files found that match the pattern \"%s\"", pattern);
		}
	}
	return result;
}

vector<OpenFileInfo> ContextFileSystem::GlobFiles(const string &pattern, const FileGlobInput &input) {
	return GlobFileList(pattern, input)->GetAllFiles();
}

string ContextFileSystem::CanonicalizePath(const string &path) {
	return GetFileSystem().CanonicalizePath(path, GetOpener());
}

string ContextFileSystem::GetHomeDirectory() {
	return FileSystem::GetHomeDirectory(GetOpener());
}

string ContextFileSystem::ExpandPath(const string &path) {
	return FileSystem::ExpandPath(path, GetOpener());
}

void ContextFileSystem::VerifyCanAccessFileInternal(const string &path, FileType type) {
	auto opener = GetOpener();
	if (!opener) {
		return;
	}
	auto db = opener->TryGetDatabase();
	if (!db) {
		return;
	}
	auto &config = db->config;
	if (!config.CanAccessFile(path, type, opener)) {
		throw PermissionException("Cannot access %s \"%s\" - file system operations are disabled by configuration",
		                          type == FileType::FILE_TYPE_DIR ? "directory" : "file", path);
	}
}

void ContextFileSystem::VerifyCanAccessFile(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_REGULAR);
}

void ContextFileSystem::VerifyCanAccessDirectory(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_DIR);
}

} // namespace duckdb
