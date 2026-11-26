#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/memory_mapped_file.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

namespace duckdb {

void FileSystem::RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs) {
	GetFileSystem().RegisterCompressionFilesystem(std::move(fs));
}

unique_ptr<FileHandle> FileSystem::OpenFile(const string &path, const FileOpenFlags &flags) {
	return OpenFile(OpenFileInfo(path), flags);
}

unique_ptr<FileHandle> FileSystem::OpenFile(const OpenFileInfo &file, const FileOpenFlags &flags) {
	VerifyCanAccessFile(file.path);
	if (IsDuckDBExtensionName(file.path)) {
		VerifyCanAccessExtension(file.path, flags);
	}
	auto fixed_fs = GetFixedFileSystem();
	if (fixed_fs) {
		return fixed_fs->OpenFile(file, flags, GetOpener());
	}
	if (flags.GetCachingMode() == CachingMode::NO_CACHING) {
		return GetFileSystem().OpenFile(file, flags, GetOpener());
	}
	return OpenCachedFile(file, flags);
}

unique_ptr<FileHandle> FileSystem::OpenCachedFile(const OpenFileInfo &file, const FileOpenFlags &flags) {
	// the cache holds the file as stored, the file system applies the compression on top of it
	auto raw_flags = flags;
	raw_flags.SetCompression(FileCompressionType::UNCOMPRESSED);
	raw_flags.SetCachingMode(CachingMode::NO_CACHING);

	auto opener = GetOpener();
	auto db = opener ? opener->TryGetDatabase() : nullptr;
	if (!db) {
		throw InvalidInputException("Cannot open \"%s\" through the external file cache without a database", file.path);
	}
	auto caching_file_system = make_shared_ptr<CachingFileSystemWrapper>(*this, *db, flags.GetCachingMode());
	auto handle = caching_file_system->OpenFile(file, raw_flags, GetOpener());
	if (!handle) {
		return nullptr;
	}
	return GetFileSystem().WrapOpenedFile(std::move(handle), file, flags, GetOpener());
}

unique_ptr<MemoryMappedFile> FileSystem::MemoryMapFile(const OpenFileInfo &path, const FileOpenFlags &flags,
                                                       const MMapOptions &options) {
	VerifyCanAccessFile(path.path);
	if (IsDuckDBExtensionName(path.path)) {
		VerifyCanAccessExtension(path.path, flags);
	}
	return FindFileSystem(path.path).MemoryMapFile(path, flags, options, GetOpener());
}

bool FileSystem::DirectoryExists(const string &directory) {
	VerifyCanAccessDirectory(directory);
	return FindFileSystem(directory).DirectoryExists(directory, GetOpener());
}

void FileSystem::CreateDirectory(const string &directory) {
	CreateDirectoryExtended(directory, {CreateDirectoryMode::SINGLE});
}

bool FileSystem::CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options) {
	if (options.mode != CreateDirectoryMode::RECURSIVE) {
		VerifyCanAccessDirectory(directory);
		return FindFileSystem(directory).CreateDirectoryExtended(directory, options, GetOpener());
	}
	// access is checked for every ancestor: walk up to the first existing directory, then create downwards
	auto parsed = Path::FromString(directory);
	vector<string> directories;
	while (!parsed.GetPathSegments().empty() && !DirectoryExists(parsed.ToString())) {
		directories.push_back(parsed.ToString());
		parsed = parsed.Parent();
	}
	if (parsed.GetPathSegments().empty() && !DirectoryExists(parsed.ToString())) {
		directories.push_back(parsed.ToString());
	}
	bool target_created = false;
	for (auto riter = directories.rbegin(); riter != directories.rend(); ++riter) {
		auto created = CreateDirectoryExtended(*riter, {CreateDirectoryMode::SINGLE});
		if (*riter == directories.front()) {
			target_created = created;
		}
	}
	return target_created;
}

void FileSystem::CreateDirectoriesRecursive(const string &path) {
	CreateDirectoryExtended(path, {CreateDirectoryMode::RECURSIVE});
}

void FileSystem::RemoveDirectory(const string &directory) {
	RemoveDirectoryExtended(directory, {RemoveDirectoryMode::RECURSIVE});
}

bool FileSystem::RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options) {
	VerifyCanAccessDirectory(directory);
	return FindFileSystem(directory).RemoveDirectoryExtended(directory, options, GetOpener());
}

void FileSystem::MoveFile(const string &source, const string &target) {
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

bool FileSystem::FileExists(const string &filename) {
	VerifyCanAccessFile(filename);
	return FindFileSystem(filename).FileExists(filename, GetOpener());
}

optional<FileMetadata> FileSystem::GetStatsIfExists(const OpenFileInfo &file) {
	VerifyCanAccessFile(file.path);
	return FindFileSystem(file.path).GetStatsIfExists(file, GetOpener());
}

bool FileSystem::IsPipe(const string &filename) {
	return FindFileSystem(filename).IsPipe(filename, GetOpener());
}

void FileSystem::RemoveFile(const string &filename) {
	VerifyCanAccessFile(filename);
	FindFileSystem(filename).RemoveFile(filename, GetOpener());
}

bool FileSystem::TryRemoveFile(const string &filename) {
	VerifyCanAccessFile(filename);
	return FindFileSystem(filename).TryRemoveFile(filename, GetOpener());
}

void FileSystem::RemoveFiles(const vector<string> &filenames) {
	reference_map_t<BaseFileSystem, vector<string>> files_by_fs;
	for (const auto &filename : filenames) {
		VerifyCanAccessFile(filename);
		files_by_fs[FindFileSystem(filename)].push_back(filename);
	}
	for (auto &entry : files_by_fs) {
		entry.first.get().RemoveFiles(entry.second, GetOpener());
	}
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	VerifyCanAccessDirectory(directory);
	return FindFileSystem(directory).ListFiles(directory, callback, GetOpener().get());
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(OpenFileInfo &info)> &callback) {
	VerifyCanAccessDirectory(directory);
	return FindFileSystem(directory).ListFiles(directory, callback, GetOpener());
}

vector<OpenFileInfo> FileSystem::Glob(const string &path) {
	VerifyCanAccessFile(path);
	return FindFileSystem(path).Glob(path, GetOpener().get());
}

unique_ptr<MultiFileList> FileSystem::Glob(const string &path, const FileGlobInput &input) {
	VerifyCanAccessFile(path);
	return FindFileSystem(path).Glob(path, input, GetOpener());
}

unique_ptr<MultiFileList> FileSystem::GlobFileList(const string &pattern, const FileGlobInput &input) {
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

vector<OpenFileInfo> FileSystem::GlobFiles(const string &pattern, const FileGlobInput &input) {
	return GlobFileList(pattern, input)->GetAllFiles();
}

string FileSystem::CanonicalizePath(const string &path) {
	return FindFileSystem(path).CanonicalizePath(path, GetOpener());
}

string FileSystem::GetHomeDirectory() {
	return FileSystem::GetHomeDirectory(GetOpener());
}

string FileSystem::ExpandPath(const string &path) {
	return FileSystem::ExpandPath(path, GetOpener());
}

BaseFileSystem &FileSystem::FindFileSystem(const string &path) {
	auto fixed_fs = GetFixedFileSystem();
	if (fixed_fs) {
		return *fixed_fs;
	}
	return GetFileSystem().FindFileSystem(path, GetOpener());
}

string FileSystem::JoinPath(const string &a, const string &path) {
	// BaseFileSystem::JoinPath is not virtual, so any file system joins the same way
	auto fixed_fs = GetFixedFileSystem();
	auto &fs = fixed_fs ? *fixed_fs : GetFileSystem().GetDefaultFileSystem();
	return fs.JoinPath(a, path);
}

BaseFileSystem &FileSystem::PathFileSystem(const string &path) const {
	auto fixed_fs = GetFixedFileSystem();
	if (fixed_fs) {
		return *fixed_fs;
	}
	return GetFileSystem().FindFileSystem(path, nullptr);
}

void FileSystem::VerifyCanAccessFileInternal(const string &path, FileType type) {
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

void FileSystem::VerifyCanAccessFile(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_REGULAR);
}

void FileSystem::VerifyCanAccessDirectory(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_DIR);
}

} // namespace duckdb
