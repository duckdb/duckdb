#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/memory_mapped_file.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

namespace duckdb {

void OpenerFileSystem::RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs) {
	GetFileSystem().RegisterCompressionFilesystem(std::move(fs));
}

unique_ptr<MemoryMappedFile> OpenerFileSystem::MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                             const MMapOptions &options,
                                                             optional_ptr<FileOpener> opener) {
	VerifyNoOpener(opener);
	VerifyCanAccessFile(path.path);
	if (IsDuckDBExtensionName(path.path)) {
		VerifyCanAccessExtension(path.path, flags);
	}
	return GetFileSystem().MemoryMapFile(path, flags, options, GetOpener());
}

unique_ptr<MultiFileList> OpenerFileSystem::GlobFilesExtended(const string &path, const FileGlobInput &input,
                                                              optional_ptr<FileOpener> opener) {
	VerifyNoOpener(opener);
	VerifyCanAccessFile(path);
	return GetFileSystem().Glob(path, input, GetOpener());
}

unique_ptr<FileHandle> OpenerFileSystem::OpenCachedFile(const OpenFileInfo &file, const FileOpenFlags &flags) {
	// the cache holds the file as stored, the file system applies the compression on top of it
	auto raw_flags = flags;
	raw_flags.SetCompression(FileCompressionType::UNCOMPRESSED);
	raw_flags.SetCachingMode(CachingMode::NO_CACHING);

	// the caching file system is kept alive by the handles it hands out
	auto caching_file_system =
	    make_shared_ptr<CachingFileSystemWrapper>(GetFileSystem(), GetOpener(), flags.GetCachingMode());
	auto handle = caching_file_system->OpenFile(file, raw_flags, GetOpener());
	if (!handle) {
		return nullptr;
	}
	return GetFileSystem().WrapOpenedFile(std::move(handle), file, flags, GetOpener());
}

void OpenerFileSystem::VerifyNoOpener(optional_ptr<FileOpener> opener) {
	if (opener) {
		throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
	}
}
void OpenerFileSystem::VerifyCanAccessFileInternal(const string &path, FileType type) {
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

void OpenerFileSystem::VerifyCanAccessFile(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_REGULAR);
}

void OpenerFileSystem::VerifyCanAccessDirectory(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_DIR);
}

} // namespace duckdb
