#include "buffered_json_reader.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

void BufferedJSONReaderOptions::Serialize(FieldWriter &writer) {
	writer.WriteString(file_path);
	writer.WriteField<JSONFormat>(format);
	writer.WriteField<FileCompressionType>(compression);
}

void BufferedJSONReaderOptions::Deserialize(FieldReader &reader) {
	file_path = reader.ReadRequired<string>();
	format = reader.ReadRequired<JSONFormat>();
	compression = reader.ReadRequired<FileCompressionType>();
}

JSONBufferHandle::JSONBufferHandle(idx_t buffer_index_p, idx_t readers_p, AllocatedData &&buffer_p, idx_t buffer_size_p)
    : buffer_index(buffer_index_p), readers(readers_p), buffer(move(buffer_p)), buffer_size(buffer_size_p) {
}

JSONFileHandle::JSONFileHandle(unique_ptr<FileHandle> file_handle_p)
    : file_handle(move(file_handle_p)), can_seek(file_handle->CanSeek()),
      plain_file_source(file_handle->OnDiskFile() && can_seek), file_size(file_handle->GetFileSize()),
      read_position(0) {
}

idx_t JSONFileHandle::FileSize() const {
	return file_size;
}

idx_t JSONFileHandle::Remaining() const {
	return file_size - read_position;
}

bool JSONFileHandle::CanSeek() const {
	return can_seek;
}

bool JSONFileHandle::PlainFileSource() const {
	return plain_file_source;
}

idx_t JSONFileHandle::GetPositionAndSize(idx_t &position, idx_t requested_size) {
	D_ASSERT(requested_size != 0);
	position = read_position;
	auto actual_size = MinValue<idx_t>(requested_size, Remaining());
	read_position += actual_size;
	return actual_size;
}

void JSONFileHandle::ReadAtPosition(const char *pointer, idx_t size, idx_t position) {
	D_ASSERT(size != 0);
	file_handle->Read((void *)pointer, size, position);
}

idx_t JSONFileHandle::Read(const char *pointer, idx_t requested_size) {
	D_ASSERT(requested_size != 0);
	auto actual_size = file_handle->Read((void *)pointer, requested_size);
	read_position += actual_size;
	return actual_size;
}

BufferedJSONReader::BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options_p, idx_t file_index_p,
                                       string file_path_p)
    : file_index(file_index_p), file_path(std::move(file_path_p)), context(context), options(std::move(options_p)),
      buffer_index(0) {
}

void BufferedJSONReader::OpenJSONFile() {
	lock_guard<mutex> guard(lock);
	auto &file_system = FileSystem::GetFileSystem(context);
	auto file_opener = FileOpener::Get(context);
	auto regular_file_handle = file_system.OpenFile(file_path.c_str(), FileFlags::FILE_FLAGS_READ,
	                                                FileLockType::NO_LOCK, options.compression, file_opener);
	file_handle = make_unique<JSONFileHandle>(std::move(regular_file_handle));
}

bool BufferedJSONReader::IsOpen() {
	return file_handle != nullptr;
}

BufferedJSONReaderOptions &BufferedJSONReader::GetOptions() {
	return options;
}

JSONFileHandle &BufferedJSONReader::GetFileHandle() const {
	return *file_handle;
}

void BufferedJSONReader::InsertBuffer(idx_t buffer_idx, unique_ptr<JSONBufferHandle> &&buffer) {
	lock_guard<mutex> guard(lock);
	buffer_map.insert(make_pair(buffer_idx, std::move(buffer)));
}

JSONBufferHandle *BufferedJSONReader::GetBuffer(idx_t buffer_idx) {
	lock_guard<mutex> guard(lock);
	auto it = buffer_map.find(buffer_idx);
	return it == buffer_map.end() ? nullptr : it->second.get();
}

AllocatedData BufferedJSONReader::RemoveBuffer(idx_t buffer_idx) {
	lock_guard<mutex> guard(lock);
	auto it = buffer_map.find(buffer_idx);
	D_ASSERT(it != buffer_map.end());
	auto result = std::move(it->second->buffer);
	buffer_map.erase(it);
	return result;
}

idx_t BufferedJSONReader::GetBufferIndex() {
	return buffer_index++;
}

double BufferedJSONReader::GetProgress() const {
	if (file_handle) {
		return 100.0 * double(file_handle->Remaining()) / double(file_handle->FileSize());
	} else {
		return 0;
	}
}

} // namespace duckdb
