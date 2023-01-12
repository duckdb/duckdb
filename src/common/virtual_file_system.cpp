#include "duckdb/common/virtual_file_system.hpp"

#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

VirtualFileSystem::VirtualFileSystem() : default_fs(FileSystem::CreateLocal()) {
	RegisterSubSystem(FileCompressionType::GZIP, make_unique<GZipFileSystem>());
}

unique_ptr<FileHandle> VirtualFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                   FileCompressionType compression, FileOpener *opener) {
	if (compression == FileCompressionType::AUTO_DETECT) {
		// auto detect compression settings based on file name
		auto lower_path = StringUtil::Lower(path);
		if (StringUtil::EndsWith(lower_path, ".gz")) {
			compression = FileCompressionType::GZIP;
		} else if (StringUtil::EndsWith(lower_path, ".zst")) {
			compression = FileCompressionType::ZSTD;
		} else {
			compression = FileCompressionType::UNCOMPRESSED;
		}
	}
	// open the base file handle
	auto file_handle = FindFileSystem(path)->OpenFile(path, flags, lock, FileCompressionType::UNCOMPRESSED, opener);
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(std::move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		auto entry = compressed_fs.find(compression);
		if (entry == compressed_fs.end()) {
			throw NotImplementedException(
			    "Attempting to open a compressed file, but the compression type is not supported");
		}
		file_handle = entry->second->OpenCompressedFile(std::move(file_handle), flags & FileFlags::FILE_FLAGS_WRITE);
	}
	return file_handle;
}

} // namespace duckdb
