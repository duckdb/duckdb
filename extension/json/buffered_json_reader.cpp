#include "buffered_json_reader.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

void BufferedJSONReaderOptions::Serialize(FieldWriter &writer) {
	writer.WriteList<string>(file_paths);
	writer.WriteField<JSONFormat>(format);
	writer.WriteField<bool>(return_json_strings);
	writer.WriteField<FileCompressionType>(compression);
	writer.WriteField<bool>(ignore_errors);
	writer.WriteField<idx_t>(maximum_object_size);
}

void BufferedJSONReaderOptions::Deserialize(FieldReader &reader) {
	file_paths = reader.ReadRequiredList<string>();
	format = reader.ReadRequired<JSONFormat>();
	return_json_strings = reader.ReadRequired<bool>();
	compression = reader.ReadRequired<FileCompressionType>();
	ignore_errors = reader.ReadRequired<bool>();
	maximum_object_size = reader.ReadRequired<idx_t>();
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
	position = read_position;
	auto actual_size = MinValue<idx_t>(requested_size, Remaining());
	read_position += actual_size;
	return actual_size;
}

void JSONFileHandle::ReadAtPosition(const char *pointer, idx_t size, idx_t position) {
	file_handle->Read((void *)pointer, size, position);
}

idx_t JSONFileHandle::Read(const char *pointer, idx_t requested_size) {
	auto actual_size = file_handle->Read((void *)pointer, requested_size);
	read_position += actual_size;
	return actual_size;
}

BufferedJSONReader::BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options)
    : options(move(options)), context(context), next_file_idx(0) {
	file_handles.reserve(options.file_paths.size());
}

void BufferedJSONReader::OpenJSONFile() {
	auto &file_system = FileSystem::GetFileSystem(context);
	auto file_opener = FileOpener::Get(context);
	auto &next_file_path = options.file_paths[next_file_idx++];
	auto regular_file_handle = file_system.OpenFile(next_file_path.c_str(), FileFlags::FILE_FLAGS_READ,
	                                                FileLockType::NO_LOCK, options.compression, file_opener);
	file_handles.emplace_back(make_unique<JSONFileHandle>(move(regular_file_handle)));
}

idx_t BufferedJSONReader::GetFileIndex() {
	return next_file_idx - 1;
}

JSONFileHandle &BufferedJSONReader::GetFileHandle(idx_t file_idx) const {
	return *file_handles[file_idx].get();
}

double BufferedJSONReader::GetProgress() const {
	if (options.file_paths.size() == 1) {
		auto &fh = *file_handles[0];
		return 100.0 * double(fh.Remaining()) / double(fh.FileSize());
	} else {
		return double(next_file_idx) / options.file_paths.size();
	}
}

idx_t BufferedJSONReader::MaxThreads(idx_t buffer_capacity) const {
	auto &fh = GetFileHandle(0);
	if (fh.CanSeek()) {
		return (fh.FileSize() + buffer_capacity - 1) / buffer_capacity;
	} else {
		return 1;
	}
}

} // namespace duckdb
