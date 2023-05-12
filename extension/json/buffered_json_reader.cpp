#include "buffered_json_reader.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/file_opener.hpp"
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
    : buffer_index(buffer_index_p), readers(readers_p), buffer(std::move(buffer_p)), buffer_size(buffer_size_p) {
}

JSONFileHandle::JSONFileHandle(duckdb::unique_ptr<FileHandle> file_handle_p, Allocator &allocator_p)
    : file_handle(std::move(file_handle_p)), allocator(allocator_p), can_seek(file_handle->CanSeek()),
      plain_file_source(file_handle->OnDiskFile() && can_seek), file_size(file_handle->GetFileSize()), read_position(0),
      requested_reads(0), actual_reads(0), cached_size(0) {
}

void JSONFileHandle::Close() {
	file_handle->Close();
	cached_buffers.clear();
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

void JSONFileHandle::Seek(idx_t position) {
	file_handle->Seek(position);
}

idx_t JSONFileHandle::GetPositionAndSize(idx_t &position, idx_t requested_size) {
	D_ASSERT(requested_size != 0);
	position = read_position;
	auto actual_size = MinValue<idx_t>(requested_size, Remaining());
	read_position += actual_size;
	if (actual_size != 0) {
		requested_reads++;
	}
	return actual_size;
}

void JSONFileHandle::ReadAtPosition(const char *pointer, idx_t size, idx_t position, bool sample_run) {
	D_ASSERT(size != 0);
	if (plain_file_source) {
		file_handle->Read((void *)pointer, size, position);
		actual_reads++;
		return;
	}

	if (sample_run) { // Cache the buffer
		file_handle->Read((void *)pointer, size, position);
		actual_reads++;
		cached_buffers.emplace_back(allocator.Allocate(size));
		memcpy(cached_buffers.back().get(), pointer, size);
		cached_size += size;
		return;
	}

	if (!cached_buffers.empty() || position < cached_size) {
		ReadFromCache(pointer, size, position);
		actual_reads++;
	}
	if (size != 0) {
		file_handle->Read((void *)pointer, size, position);
		actual_reads++;
	}
}

idx_t JSONFileHandle::Read(const char *pointer, idx_t requested_size, bool sample_run) {
	D_ASSERT(requested_size != 0);
	if (plain_file_source) {
		auto actual_size = ReadInternal(pointer, requested_size);
		read_position += actual_size;
		return actual_size;
	}

	if (sample_run) { // Cache the buffer
		auto actual_size = ReadInternal(pointer, requested_size);
		if (actual_size > 0) {
			cached_buffers.emplace_back(allocator.Allocate(actual_size));
			memcpy(cached_buffers.back().get(), pointer, actual_size);
		}
		cached_size += actual_size;
		read_position += actual_size;
		return actual_size;
	}

	idx_t actual_size = 0;
	if (!cached_buffers.empty() || read_position < cached_size) {
		actual_size += ReadFromCache(pointer, requested_size, read_position);
	}
	if (requested_size != 0) {
		actual_size += ReadInternal(pointer, requested_size);
	}
	return actual_size;
}

idx_t JSONFileHandle::ReadFromCache(const char *&pointer, idx_t &size, idx_t &position) {
	idx_t read_size = 0;
	idx_t total_offset = 0;

	idx_t cached_buffer_idx;
	for (cached_buffer_idx = 0; cached_buffer_idx < cached_buffers.size(); cached_buffer_idx++) {
		auto &cached_buffer = cached_buffers[cached_buffer_idx];
		if (size == 0) {
			break;
		}
		if (position < total_offset + cached_buffer.GetSize()) {
			idx_t within_buffer_offset = position - total_offset;
			idx_t copy_size = MinValue<idx_t>(size, cached_buffer.GetSize() - within_buffer_offset);
			memcpy((void *)pointer, cached_buffer.get() + within_buffer_offset, copy_size);

			read_size += copy_size;
			pointer += copy_size;
			size -= copy_size;
			position += copy_size;
		}
		total_offset += cached_buffer.GetSize();
	}

	return read_size;
}

idx_t JSONFileHandle::ReadInternal(const char *pointer, const idx_t requested_size) {
	// Deal with reading from pipes
	idx_t total_read_size = 0;
	while (total_read_size < requested_size) {
		auto read_size = file_handle->Read((void *)(pointer + total_read_size), requested_size - total_read_size);
		if (read_size == 0) {
			break;
		}
		total_read_size += read_size;
	}
	return total_read_size;
}

BufferedJSONReader::BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options_p, string file_path_p)
    : file_path(std::move(file_path_p)), context(context), options(std::move(options_p)), buffer_index(0) {
}

void BufferedJSONReader::OpenJSONFile() {
	lock_guard<mutex> guard(lock);
	auto &file_system = FileSystem::GetFileSystem(context);
	auto regular_file_handle =
	    file_system.OpenFile(file_path.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK, options.compression);
	file_handle = make_uniq<JSONFileHandle>(std::move(regular_file_handle), BufferAllocator::Get(context));
}

void BufferedJSONReader::CloseJSONFile() {
	while (true) {
		lock_guard<mutex> guard(lock);
		if (file_handle->RequestedReadsComplete()) {
			file_handle->Close();
			break;
		}
	}
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

void BufferedJSONReader::InsertBuffer(idx_t buffer_idx, duckdb::unique_ptr<JSONBufferHandle> &&buffer) {
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
	buffer_line_or_object_counts.push_back(-1);
	return buffer_index++;
}

void BufferedJSONReader::SetBufferLineOrObjectCount(idx_t index, idx_t count) {
	lock_guard<mutex> guard(lock);
	buffer_line_or_object_counts[index] = count;
}

idx_t BufferedJSONReader::GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf) {
	D_ASSERT(options.format == JSONFormat::UNSTRUCTURED || options.format == JSONFormat::NEWLINE_DELIMITED);
	while (true) {
		lock_guard<mutex> guard(lock);
		idx_t line = line_or_object_in_buf;
		bool can_throw = true;
		for (idx_t b_idx = 0; b_idx < buf_index; b_idx++) {
			if (buffer_line_or_object_counts[b_idx] == -1) {
				can_throw = false;
				break;
			} else {
				line += buffer_line_or_object_counts[b_idx];
			}
		}
		if (!can_throw) {
			continue;
		}
		// SQL uses 1-based indexing so I guess we will do that in our exception here as well
		return line + 1;
	}
}

void BufferedJSONReader::ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf, yyjson_read_err &err,
                                         const string &extra) {
	string unit = options.format == JSONFormat::NEWLINE_DELIMITED ? "line" : "object";
	auto line = GetLineNumber(buf_index, line_or_object_in_buf);
	throw InvalidInputException("Malformed JSON in file \"%s\", at byte %llu in %s %llu: %s. %s", file_path,
	                            err.pos + 1, unit, line + 1, err.msg, extra);
}

void BufferedJSONReader::ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf,
                                             const string &error_message) {
	string unit = options.format == JSONFormat::NEWLINE_DELIMITED ? "line" : "object";
	auto line = GetLineNumber(buf_index, line_or_object_in_buf);
	throw InvalidInputException("JSON transform error in file \"%s\", in %s %llu: %s", file_path, unit, line,
	                            error_message);
}

double BufferedJSONReader::GetProgress() const {
	if (file_handle) {
		return 100.0 * double(file_handle->Remaining()) / double(file_handle->FileSize());
	} else {
		return 0;
	}
}

void BufferedJSONReader::Reset() {
	buffer_index = 0;
	buffer_map.clear();
	buffer_line_or_object_counts.clear();

	if (file_handle->CanSeek()) {
		file_handle->Seek(0);
	} else {
		file_handle->Reset();
	}
	file_handle->Reset();
}

void JSONFileHandle::Reset() {
	read_position = 0;
	requested_reads = 0;
	actual_reads = 0;
	if (plain_file_source) {
		file_handle->Reset();
	}
}

bool JSONFileHandle::RequestedReadsComplete() {
	return requested_reads == actual_reads;
}

} // namespace duckdb
