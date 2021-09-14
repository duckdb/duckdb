#include "duckdb/common/virtual_file_system.hpp"

#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

VirtualFileSystem::VirtualFileSystem() : default_fs(FileSystem::CreateLocal()) {
}

unique_ptr<FileHandle> VirtualFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                   FileCompressionType compression) {
	if (compression == FileCompressionType::AUTO_DETECT) {
		// auto detect compression settings based on file name
		auto lower_path = StringUtil::Lower(path);
		if (StringUtil::EndsWith(lower_path, ".gz")) {
			compression = FileCompressionType::GZIP;
		} else {
			compression = FileCompressionType::UNCOMPRESSED;
		}
	}
	// open the base file handle
	auto file_handle = FindFileSystem(path)->OpenFile(path, flags, lock, FileCompressionType::UNCOMPRESSED);
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		switch (compression) {
		case FileCompressionType::GZIP:
			file_handle = GZipFileSystem::OpenCompressedFile(move(file_handle));
			break;
		default:
			throw NotImplementedException("Unimplemented compression type");
		}
	}
	return file_handle;
}

} // namespace duckdb
