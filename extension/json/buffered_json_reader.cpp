#include "buffered_json_reader.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

JSONFileHandle::JSONFileHandle(unique_ptr<FileHandle> file_handle_p)
    : file_handle(move(file_handle_p)), can_seek(file_handle->CanSeek()),
      plain_file_source(file_handle->OnDiskFile() && can_seek), file_size(file_handle->GetFileSize()),
      read_position(0) {
	if (!plain_file_source) {
		throw NotImplementedException("Non-plain file source JSON");
	}
}

idx_t JSONFileHandle::Remaining() const {
	return file_size - read_position;
}

idx_t JSONFileHandle::GetPositionAndSize(idx_t &position, idx_t requested_size) {
	position = read_position;
	auto actual_size = MinValue<idx_t>(requested_size, Remaining());
	read_position += actual_size;
	return actual_size;
}

void JSONFileHandle::Read(data_ptr_t pointer, idx_t size, idx_t position) {
	file_handle->Read((void *)pointer, size, position);
}

BufferedJSONReader::BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options)
    : context(context), options(move(options)) {
}

void BufferedJSONReader::OpenJSONFile() {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto file_opener = FileOpener::Get(context);
	auto regular_file_handle = file_system.OpenFile(options.file_path.c_str(), FileFlags::FILE_FLAGS_READ,
	                                                FileLockType::NO_LOCK, options.compression, file_opener);
	file_handle = make_unique<JSONFileHandle>(move(regular_file_handle));
}

JSONFileHandle &BufferedJSONReader::GetFileHandle() {
	return *file_handle;
}

} // namespace duckdb
