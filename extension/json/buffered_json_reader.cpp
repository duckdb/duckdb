#include "buffered_json_reader.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "json_scan.hpp"
#include <utility>

namespace duckdb {

JSONBufferHandle::JSONBufferHandle(idx_t buffer_index_p, idx_t readers_p, AllocatedData &&buffer_p, idx_t buffer_size_p)
    : buffer_index(buffer_index_p), readers(readers_p), buffer(std::move(buffer_p)), buffer_size(buffer_size_p) {
}

JSONFileHandle::JSONFileHandle(unique_ptr<FileHandle> file_handle_p, Allocator &allocator_p)
    : file_handle(std::move(file_handle_p)), allocator(allocator_p), can_seek(file_handle->CanSeek()),
      file_size(file_handle->GetFileSize()), read_position(0), requested_reads(0), actual_reads(0),
      last_read_requested(false), cached_size(0) {
}

bool JSONFileHandle::IsOpen() const {
	return file_handle != nullptr;
}

void JSONFileHandle::Close() {
	if (IsOpen() && !file_handle->IsPipe()) {
		file_handle->Close();
		file_handle = nullptr;
	}
}

void JSONFileHandle::Reset() {
	D_ASSERT(RequestedReadsComplete());
	read_position = 0;
	requested_reads = 0;
	actual_reads = 0;
	last_read_requested = false;
	if (IsOpen() && !file_handle->IsPipe()) {
		file_handle->Reset();
	}
}

bool JSONFileHandle::RequestedReadsComplete() {
	return requested_reads == actual_reads;
}

bool JSONFileHandle::LastReadRequested() const {
	return last_read_requested;
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

bool JSONFileHandle::IsPipe() const {
	return file_handle->IsPipe();
}

FileHandle &JSONFileHandle::GetHandle() {
	return *file_handle;
}

bool JSONFileHandle::GetPositionAndSize(idx_t &position, idx_t &size, idx_t requested_size) {
	D_ASSERT(requested_size != 0);
	if (last_read_requested) {
		return false;
	}

	position = read_position;
	size = MinValue<idx_t>(requested_size, Remaining());
	read_position += size;

	requested_reads++;
	if (size == 0) {
		last_read_requested = true;
	}

	return true;
}

void JSONFileHandle::ReadAtPosition(char *pointer, idx_t size, idx_t position, bool &file_done, bool sample_run,
                                    optional_ptr<FileHandle> override_handle) {
	if (size != 0) {
		auto &handle = override_handle ? *override_handle.get() : *file_handle.get();
		if (can_seek) {
			handle.Read(pointer, size, position);
		} else if (file_handle->IsPipe()) { // Cache the buffer
			handle.Read(pointer, size, position);

			cached_buffers.emplace_back(allocator.Allocate(size));
			memcpy(cached_buffers.back().get(), pointer, size);
			cached_size += size;
		} else {
			if (!cached_buffers.empty() || position < cached_size) {
				ReadFromCache(pointer, size, position);
			}

			if (size != 0) {
				handle.Read(pointer, size, position);
			}
		}
	}

	const auto incremented_actual_reads = ++actual_reads;
	if (incremented_actual_reads > requested_reads) {
		throw InternalException("JSONFileHandle performed more actual reads than requested reads");
	}

	if (last_read_requested && incremented_actual_reads == requested_reads) {
		file_done = true;
	}
}

bool JSONFileHandle::Read(char *pointer, idx_t &read_size, idx_t requested_size, bool &file_done, bool sample_run) {
	D_ASSERT(requested_size != 0);
	if (last_read_requested) {
		return false;
	}

	if (can_seek) {
		read_size = ReadInternal(pointer, requested_size);
		read_position += read_size;
	} else if (file_handle->IsPipe()) { // Cache the buffer
		read_size = ReadInternal(pointer, requested_size);
		if (read_size > 0) {
			cached_buffers.emplace_back(allocator.Allocate(read_size));
			memcpy(cached_buffers.back().get(), pointer, read_size);
		}
		cached_size += read_size;
		read_position += read_size;
	} else {
		read_size = 0;
		if (!cached_buffers.empty() || read_position < cached_size) {
			read_size += ReadFromCache(pointer, requested_size, read_position);
		}
		if (requested_size != 0) {
			read_size += ReadInternal(pointer, requested_size);
		}
	}

	if (read_size == 0) {
		last_read_requested = true;
		file_done = true;
	}

	return true;
}

idx_t JSONFileHandle::ReadInternal(char *pointer, const idx_t requested_size) {
	// Deal with reading from pipes
	idx_t total_read_size = 0;
	while (total_read_size < requested_size) {
		auto read_size = file_handle->Read(pointer + total_read_size, requested_size - total_read_size);
		if (read_size == 0) {
			break;
		}
		total_read_size += read_size;
	}
	return total_read_size;
}

idx_t JSONFileHandle::ReadFromCache(char *&pointer, idx_t &size, idx_t &position) {
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
			memcpy(pointer, cached_buffer.get() + within_buffer_offset, copy_size);

			read_size += copy_size;
			pointer += copy_size;
			size -= copy_size;
			position += copy_size;
		}
		total_offset += cached_buffer.GetSize();
	}

	return read_size;
}

BufferedJSONReader::BufferedJSONReader(ClientContext &context, JSONReaderOptions options_p, string file_name_p)
    : BaseFileReader(std::move(file_name_p)), context(context), options(std::move(options_p)), buffer_index(0),
      thrown(false) {
}

void BufferedJSONReader::OpenJSONFile() {
	lock_guard<mutex> guard(lock);
	if (!IsOpen()) {
		auto &fs = FileSystem::GetFileSystem(context);
		auto regular_file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ | options.compression);
		file_handle = make_uniq<JSONFileHandle>(std::move(regular_file_handle), BufferAllocator::Get(context));
	}
	Reset();
}

void BufferedJSONReader::Reset() {
	buffer_index = 0;
	buffer_map.clear();
	buffer_line_or_object_counts.clear();
	if (HasFileHandle()) {
		file_handle->Reset();
	}
}

bool BufferedJSONReader::HasFileHandle() const {
	return file_handle != nullptr;
}

bool BufferedJSONReader::IsOpen() const {
	if (HasFileHandle()) {
		return file_handle->IsOpen();
	}
	return false;
}

JSONReaderOptions &BufferedJSONReader::GetOptions() {
	return options;
}

JSONFormat BufferedJSONReader::GetFormat() const {
	return options.format;
}

void BufferedJSONReader::SetFormat(JSONFormat format) {
	D_ASSERT(options.format == JSONFormat::AUTO_DETECT);
	options.format = format;
}

JSONRecordType BufferedJSONReader::GetRecordType() const {
	return options.record_type;
}

void BufferedJSONReader::SetRecordType(duckdb::JSONRecordType type) {
	D_ASSERT(options.record_type == JSONRecordType::AUTO_DETECT);
	options.record_type = type;
}

const string &BufferedJSONReader::GetFileName() const {
	return file_name;
}

JSONFileHandle &BufferedJSONReader::GetFileHandle() const {
	D_ASSERT(HasFileHandle());
	return *file_handle;
}

void BufferedJSONReader::InsertBuffer(idx_t buffer_idx, unique_ptr<JSONBufferHandle> &&buffer) {
	lock_guard<mutex> guard(lock);
	buffer_map.insert(make_pair(buffer_idx, std::move(buffer)));
}

optional_ptr<JSONBufferHandle> BufferedJSONReader::GetBuffer(idx_t buffer_idx) {
	lock_guard<mutex> guard(lock);
	auto it = buffer_map.find(buffer_idx);
	return it == buffer_map.end() ? nullptr : it->second.get();
}

AllocatedData BufferedJSONReader::RemoveBuffer(JSONBufferHandle &handle) {
	lock_guard<mutex> guard(lock);
	auto it = buffer_map.find(handle.buffer_index);
	D_ASSERT(it != buffer_map.end());
	D_ASSERT(RefersToSameObject(handle, *it->second));
	auto result = std::move(it->second->buffer);
	buffer_map.erase(it);
	return result;
}

idx_t BufferedJSONReader::GetBufferIndex() {
	buffer_line_or_object_counts.push_back(-1);
	return buffer_index++;
}

void BufferedJSONReader::SetBufferLineOrObjectCount(JSONBufferHandle &handle, idx_t count) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(buffer_map.find(handle.buffer_index) != buffer_map.end());
	D_ASSERT(RefersToSameObject(handle, *buffer_map.find(handle.buffer_index)->second));
	D_ASSERT(buffer_line_or_object_counts[handle.buffer_index] == -1);
	buffer_line_or_object_counts[handle.buffer_index] = count;
}

idx_t BufferedJSONReader::GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf) {
	D_ASSERT(options.format != JSONFormat::AUTO_DETECT);
	while (true) {
		idx_t line = line_or_object_in_buf;
		bool can_throw = true;
		{
			lock_guard<mutex> guard(lock);
			if (thrown) {
				return DConstants::INVALID_INDEX;
			}
			for (idx_t b_idx = 0; b_idx < buf_index; b_idx++) {
				if (buffer_line_or_object_counts[b_idx] == -1) {
					can_throw = false;
					break;
				} else {
					line += buffer_line_or_object_counts[b_idx];
				}
			}
			if (can_throw) {
				thrown = true;
				// SQL uses 1-based indexing so I guess we will do that in our exception here as well
				return line + 1;
			}
		}
		TaskScheduler::YieldThread();
	}
}

void BufferedJSONReader::ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf, yyjson_read_err &err,
                                         const string &extra) {
	string unit = options.format == JSONFormat::NEWLINE_DELIMITED ? "line" : "record/value";
	auto line = GetLineNumber(buf_index, line_or_object_in_buf);
	throw InvalidInputException("Malformed JSON in file \"%s\", at byte %llu in %s %llu: %s. %s", file_name,
	                            err.pos + 1, unit, line + 1, err.msg, extra);
}

void BufferedJSONReader::ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf,
                                             const string &error_message) {
	string unit = options.format == JSONFormat::NEWLINE_DELIMITED ? "line" : "record/value";
	auto line = GetLineNumber(buf_index, line_or_object_in_buf);
	throw InvalidInputException("JSON transform error in file \"%s\", in %s %llu: %s", file_name, unit, line,
	                            error_message);
}

bool BufferedJSONReader::HasThrown() {
	lock_guard<mutex> guard(lock);
	return thrown;
}

double BufferedJSONReader::GetProgress() const {
	lock_guard<mutex> guard(lock);
	if (HasFileHandle()) {
		return 100.0 - 100.0 * double(file_handle->Remaining()) / double(file_handle->FileSize());
	} else {
		return 0;
	}
}

static inline void TrimWhitespace(JSONString &line) {
	while (line.size != 0 && StringUtil::CharacterIsSpace(line[0])) {
		line.pointer++;
		line.size--;
	}
	while (line.size != 0 && StringUtil::CharacterIsSpace(line[line.size - 1])) {
		line.size--;
	}
}

JSONReaderScanState::JSONReaderScanState(ClientContext &context, Allocator &global_allocator, idx_t buffer_capacity)
    : fs(FileSystem::GetFileSystem(context)), global_allocator(global_allocator), allocator(BufferAllocator::Get(context)),
      buffer_capacity(buffer_capacity) {
}

void JSONReaderScanState::Reset() {
	allocator.Reset();
	scan_count = 0;
}

data_ptr_t JSONReaderScanState::GetReconstructBuffer() {
	if (!reconstruct_buffer.IsSet()) {
		reconstruct_buffer = global_allocator.Allocate(buffer_capacity);
	}
	return reconstruct_buffer.get();
}

static inline void SkipWhitespace(const char *buffer_ptr, idx_t &buffer_offset, const idx_t &buffer_size) {
	for (; buffer_offset != buffer_size; buffer_offset++) {
		if (!StringUtil::CharacterIsSpace(buffer_ptr[buffer_offset])) {
			break;
		}
	}
}

static inline const char *NextNewline(const char *ptr, const idx_t size) {
	return const_char_ptr_cast(memchr(ptr, '\n', size));
}

static inline const char *PreviousNewline(const char *ptr, const idx_t size) {
	const auto end = ptr - size;
	for (ptr--; ptr != end; ptr--) {
		if (*ptr == '\n') {
			break;
		}
	}
	return ptr;
}

static inline const char *NextJSONDefault(const char *ptr, const char *const end) {
	idx_t parents = 0;
	while (ptr != end) {
		switch (*ptr++) {
		case '{':
		case '[':
			parents++;
			continue;
		case '}':
		case ']':
			parents--;
			break;
		case '"':
			while (ptr != end) {
				auto string_char = *ptr++;
				if (string_char == '"') {
					break;
				} else if (string_char == '\\') {
					if (ptr != end) {
						ptr++; // Skip the escaped char
					}
				}
			}
			break;
		default:
			continue;
		}

		if (parents == 0) {
			break;
		}
	}

	return ptr;
}

static inline const char *NextJSON(const char *ptr, const idx_t size) {
	D_ASSERT(!StringUtil::CharacterIsSpace(*ptr)); // Should be handled before

	const char *const end = ptr + size;
	switch (*ptr) {
	case '{':
	case '[':
	case '"':
		ptr = NextJSONDefault(ptr, end);
		break;
	default:
		// Special case: JSON array containing JSON without clear "parents", i.e., not obj/arr/str
		while (ptr != end) {
			switch (*ptr++) {
			case ',':
			case ']':
				ptr--;
				break;
			default:
				continue;
			}
			break;
		}
	}

	return ptr == end ? nullptr : ptr;
}

void BufferedJSONReader::SkipOverArrayStart(JSONReaderScanState &scan_state) {
	// First read of this buffer, check if it's actually an array and skip over the bytes
	auto &buffer_ptr = scan_state.buffer_ptr;
	auto &buffer_offset = scan_state.buffer_offset;
	auto &buffer_size = scan_state.buffer_size;
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	if (buffer_offset == buffer_size) {
		return; // Empty file
	}
	if (buffer_ptr[buffer_offset] != '[') {
		throw InvalidInputException(
		    "Expected top-level JSON array with format='array', but first character is '%c' in file \"%s\"."
		    "\n Try setting format='auto' or format='newline_delimited'.",
		    buffer_ptr[buffer_offset], GetFileName());
	}
	SkipWhitespace(buffer_ptr, ++buffer_offset, buffer_size);
	if (buffer_offset >= buffer_size) {
		throw InvalidInputException("Missing closing brace ']' in JSON array with format='array' in file \"%s\"",
		                            GetFileName());
	}
	if (buffer_ptr[buffer_offset] == ']') {
		// Empty array
		SkipWhitespace(buffer_ptr, ++buffer_offset, buffer_size);
		if (buffer_offset != buffer_size) {
			throw InvalidInputException(
			    "Empty array with trailing data when parsing JSON array with format='array' in file \"%s\"",
			    GetFileName());
		}
	}
}

void BufferedJSONReader::ThrowTransformError(JSONReaderScanState &scan_state, idx_t object_index,
                                             const string &error_message) {
	D_ASSERT(scan_state.current_buffer_handle);
	D_ASSERT(object_index != DConstants::INVALID_INDEX);
	auto line_or_object_in_buffer = scan_state.lines_or_objects_in_buffer - scan_state.scan_count + object_index;
	ThrowTransformError(scan_state.current_buffer_handle->buffer_index, line_or_object_in_buffer, error_message);
}

static pair<JSONFormat, JSONRecordType> DetectFormatAndRecordType(char *const buffer_ptr, const idx_t buffer_size,
                                                                  yyjson_alc *alc) {
	// First we do the easy check whether it's NEWLINE_DELIMITED
	auto line_end = NextNewline(buffer_ptr, buffer_size);
	if (line_end != nullptr) {
		idx_t line_size = line_end - buffer_ptr;
		SkipWhitespace(buffer_ptr, line_size, buffer_size);

		yyjson_read_err error;
		auto doc = JSONCommon::ReadDocumentUnsafe(buffer_ptr, line_size, JSONCommon::READ_FLAG, alc, &error);
		if (error.code == YYJSON_READ_SUCCESS) { // We successfully read the line
			if (yyjson_is_arr(doc->root) && line_size == buffer_size) {
				// It's just one array, let's actually assume ARRAY, not NEWLINE_DELIMITED
				if (yyjson_arr_size(doc->root) == 0 || yyjson_is_obj(yyjson_arr_get(doc->root, 0))) {
					// Either an empty array (assume records), or an array of objects
					return make_pair(JSONFormat::ARRAY, JSONRecordType::RECORDS);
				} else {
					return make_pair(JSONFormat::ARRAY, JSONRecordType::VALUES);
				}
			} else if (yyjson_is_obj(doc->root)) {
				return make_pair(JSONFormat::NEWLINE_DELIMITED, JSONRecordType::RECORDS);
			} else {
				return make_pair(JSONFormat::NEWLINE_DELIMITED, JSONRecordType::VALUES);
			}
		}
	}

	// Skip whitespace
	idx_t buffer_offset = 0;
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	auto remaining = buffer_size - buffer_offset;

	// We know it's not NEWLINE_DELIMITED at this point, if there's a '{', we know it's not ARRAY either
	// Also if it's fully whitespace we just return something because we don't know
	if (remaining == 0 || buffer_ptr[buffer_offset] == '{') {
		return make_pair(JSONFormat::UNSTRUCTURED, JSONRecordType::RECORDS);
	}

	// We know it's not top-level records, if it's not '[', it's not ARRAY either
	if (buffer_ptr[buffer_offset] != '[') {
		return make_pair(JSONFormat::UNSTRUCTURED, JSONRecordType::VALUES);
	}

	// It's definitely an ARRAY, but now we have to figure out if there's more than one top-level array
	yyjson_read_err error;
	auto doc =
	    JSONCommon::ReadDocumentUnsafe(buffer_ptr + buffer_offset, remaining, JSONCommon::READ_STOP_FLAG, alc, &error);
	if (error.code == YYJSON_READ_SUCCESS) {
		D_ASSERT(yyjson_is_arr(doc->root));

		// We successfully read something!
		buffer_offset += yyjson_doc_get_read_size(doc);
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
		remaining = buffer_size - buffer_offset;

		if (remaining != 0) { // There's more
			return make_pair(JSONFormat::UNSTRUCTURED, JSONRecordType::VALUES);
		}

		// Just one array, check what's in there
		if (yyjson_arr_size(doc->root) == 0 || yyjson_is_obj(yyjson_arr_get(doc->root, 0))) {
			// Either an empty array (assume records), or an array of objects
			return make_pair(JSONFormat::ARRAY, JSONRecordType::RECORDS);
		} else {
			return make_pair(JSONFormat::ARRAY, JSONRecordType::VALUES);
		}
	}

	// We weren't able to parse an array, could be broken or an array larger than our buffer size, let's skip over '['
	SkipWhitespace(buffer_ptr, ++buffer_offset, --remaining);
	remaining = buffer_size - buffer_offset;

	// If it's '{' we know there's RECORDS in the ARRAY, else it's VALUES
	if (remaining == 0 || buffer_ptr[buffer_offset] == '{') {
		return make_pair(JSONFormat::ARRAY, JSONRecordType::RECORDS);
	}

	// It's not RECORDS, so it must be VALUES
	return make_pair(JSONFormat::ARRAY, JSONRecordType::VALUES);
}

void BufferedJSONReader::ParseJSON(JSONReaderScanState &scan_state, char *const json_start, const idx_t json_size,
                                   const idx_t remaining) {
	yyjson_doc *doc;
	yyjson_read_err err;
	if (options.type == JSONScanType::READ_JSON_OBJECTS) { // If we return strings, we cannot parse INSITU
		doc = JSONCommon::ReadDocumentUnsafe(json_start, json_size, JSONCommon::READ_STOP_FLAG,
		                                     scan_state.allocator.GetYYAlc(), &err);
	} else {
		doc = JSONCommon::ReadDocumentUnsafe(json_start, remaining, JSONCommon::READ_INSITU_FLAG,
		                                     scan_state.allocator.GetYYAlc(), &err);
	}
	if (err.code != YYJSON_READ_SUCCESS) {
		auto can_ignore_this_error = options.ignore_errors;
		string extra;
		if (GetFormat() != JSONFormat::NEWLINE_DELIMITED) {
			can_ignore_this_error = false;
			extra = options.ignore_errors
			            ? "Parse errors cannot be ignored for JSON formats other than 'newline_delimited'"
			            : "";
		}
		if (!can_ignore_this_error) {
			ThrowParseError(scan_state.current_buffer_handle->buffer_index, scan_state.lines_or_objects_in_buffer, err,
			                extra);
		}
	}

	// We parse with YYJSON_STOP_WHEN_DONE, so we need to check this by hand
	const auto read_size = yyjson_doc_get_read_size(doc);
	if (read_size > json_size) {
		// Can't go past the boundary, even with ignore_errors
		err.code = YYJSON_READ_ERROR_UNEXPECTED_END;
		err.msg = "unexpected end of data";
		err.pos = json_size;
		ThrowParseError(scan_state.current_buffer_handle->buffer_index, scan_state.lines_or_objects_in_buffer, err,
		                "Try auto-detecting the JSON format");
	} else if (!options.ignore_errors && read_size < json_size) {
		idx_t off = read_size;
		idx_t rem = json_size;
		SkipWhitespace(json_start, off, rem);
		if (off != rem) { // Between end of document and boundary should be whitespace only
			err.code = YYJSON_READ_ERROR_UNEXPECTED_CONTENT;
			err.msg = "unexpected content after document";
			err.pos = read_size;
			ThrowParseError(scan_state.current_buffer_handle->buffer_index, scan_state.lines_or_objects_in_buffer, err,
			                "Try auto-detecting the JSON format");
		}
	}

	scan_state.lines_or_objects_in_buffer++;
	if (!doc) {
		scan_state.values[scan_state.scan_count] = nullptr;
		return;
	}

	// Set the JSONLine and trim
	scan_state.units[scan_state.scan_count] = JSONString(json_start, json_size);
	TrimWhitespace(scan_state.units[scan_state.scan_count]);
	scan_state.values[scan_state.scan_count] = doc->root;
}

void BufferedJSONReader::AutoDetect(JSONReaderScanState &scan_state, optional_idx buffer_index) {
	if (scan_state.buffer_size == 0) {
		return;
	}

	auto format_and_record_type =
	    DetectFormatAndRecordType(scan_state.buffer_ptr, scan_state.buffer_size, scan_state.allocator.GetYYAlc());
	if (GetFormat() == JSONFormat::AUTO_DETECT) {
		SetFormat(format_and_record_type.first);
	}
	if (GetRecordType() == JSONRecordType::AUTO_DETECT) {
		SetRecordType(format_and_record_type.second);
	}
	if (GetFormat() == JSONFormat::ARRAY) {
		SkipOverArrayStart(scan_state);
	}

	if (!options.ignore_errors && options.record_type == JSONRecordType::RECORDS &&
	    GetRecordType() != JSONRecordType::RECORDS) {
		ThrowTransformError(buffer_index.GetIndex(), 0, "Expected records, detected non-record JSON instead.");
	}
}

void BufferedJSONReader::ThrowObjectSizeError(const idx_t object_size) {
	throw InvalidInputException(
	    "\"maximum_object_size\" of %llu bytes exceeded while reading file \"%s\" (>%llu bytes)."
	    "\n Try increasing \"maximum_object_size\".",
	    options.maximum_object_size, GetFileName(), object_size);
}

bool BufferedJSONReader::ReconstructFirstObject(JSONReaderScanState &scan_state) {

	D_ASSERT(scan_state.current_buffer_handle->buffer_index != 0);
	D_ASSERT(GetFormat() == JSONFormat::NEWLINE_DELIMITED);

	// Spinlock until the previous batch index has also read its buffer
	optional_ptr<JSONBufferHandle> previous_buffer_handle;
	while (!previous_buffer_handle) {
		if (HasThrown()) {
			return false;
		}
		previous_buffer_handle = GetBuffer(scan_state.current_buffer_handle->buffer_index - 1);
	}

	// First we find the newline in the previous block
	auto prev_buffer_ptr = char_ptr_cast(previous_buffer_handle->buffer.get()) + previous_buffer_handle->buffer_size;
	auto part1_ptr = PreviousNewline(prev_buffer_ptr, previous_buffer_handle->buffer_size);
	auto part1_size = prev_buffer_ptr - part1_ptr;

	// Now copy the data to our reconstruct buffer
	const auto reconstruct_ptr = scan_state.GetReconstructBuffer();
	memcpy(reconstruct_ptr, part1_ptr, part1_size);

	// We copied the object, so we are no longer reading the previous buffer
	if (--previous_buffer_handle->readers == 0) {
		RemoveBuffer(*previous_buffer_handle);
	}

	if (part1_size == 1) {
		// Just a newline
		return false;
	}

	idx_t line_size = part1_size;
	if (scan_state.buffer_size != 0) {
		// Now find the newline in the current block
		auto line_end = NextNewline(scan_state.buffer_ptr, scan_state.buffer_size);
		if (line_end == nullptr) {
			ThrowObjectSizeError(scan_state.buffer_size - scan_state.buffer_offset);
		} else {
			line_end++;
		}
		idx_t part2_size = line_end - scan_state.buffer_ptr;

		line_size += part2_size;
		if (line_size > options.maximum_object_size) {
			ThrowObjectSizeError(line_size);
		}

		// And copy the remainder of the line to the reconstruct buffer
		memcpy(reconstruct_ptr + part1_size, scan_state.buffer_ptr, part2_size);
		memset(reconstruct_ptr + line_size, 0, YYJSON_PADDING_SIZE);
		scan_state.buffer_offset += part2_size;
	}

	ParseJSON(scan_state, char_ptr_cast(reconstruct_ptr), line_size, line_size);

	return true;
}

void BufferedJSONReader::ParseNextChunk(JSONReaderScanState &scan_state) {
	const auto format = GetFormat();
	auto &buffer_ptr = scan_state.buffer_ptr;
	auto &buffer_offset = scan_state.buffer_offset;
	auto &buffer_size = scan_state.buffer_size;
	auto &scan_count = scan_state.scan_count;
	for (; scan_count < STANDARD_VECTOR_SIZE; scan_count++) {
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
		auto json_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;
		if (remaining == 0) {
			break;
		}
		D_ASSERT(format != JSONFormat::AUTO_DETECT);
		const char *json_end = format == JSONFormat::NEWLINE_DELIMITED ? NextNewline(json_start, remaining)
		                                                               : NextJSON(json_start, remaining);
		if (json_end == nullptr) {
			// We reached the end of the buffer
			if (!scan_state.is_last) {
				// Last bit of data belongs to the next batch
				if (format != JSONFormat::NEWLINE_DELIMITED) {
					if (remaining > options.maximum_object_size) {
						ThrowObjectSizeError(remaining);
					}
					memcpy(scan_state.GetReconstructBuffer(), json_start, remaining);
					scan_state.prev_buffer_remainder = remaining;
				}
				buffer_offset = buffer_size;
				break;
			}
			json_end = json_start + remaining;
		}

		idx_t json_size = json_end - json_start;
		ParseJSON(scan_state, json_start, json_size, remaining);
		buffer_offset += json_size;

		if (format == JSONFormat::ARRAY) {
			SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
			if (buffer_ptr[buffer_offset] == ',' || buffer_ptr[buffer_offset] == ']') {
				buffer_offset++;
			} else { // We can't ignore this error, even with 'ignore_errors'
				yyjson_read_err err;
				err.code = YYJSON_READ_ERROR_UNEXPECTED_CHARACTER;
				err.msg = "unexpected character";
				err.pos = json_size;
				ThrowParseError(scan_state.current_buffer_handle->buffer_index, scan_state.lines_or_objects_in_buffer,
				                err);
			}
		}
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}
}

bool BufferedJSONReader::ReadNextBuffer(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer, optional_idx &buffer_index, bool &file_done) {
	// Try to re-use a buffer that was used before
	if (scan_state.current_buffer_handle) {
		SetBufferLineOrObjectCount(*scan_state.current_buffer_handle,
												   scan_state.lines_or_objects_in_buffer);
		if (--scan_state.current_buffer_handle->readers == 0) {
			buffer = RemoveBuffer(*scan_state.current_buffer_handle);
		}
	}

	// Copy last bit of previous buffer
	if (GetFormat() != JSONFormat::NEWLINE_DELIMITED && !scan_state.is_last) {
		memcpy(scan_state.buffer_ptr, scan_state.GetReconstructBuffer(), scan_state.prev_buffer_remainder);
	}
	if (!buffer.IsSet()) {
		buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
		scan_state.buffer_ptr = char_ptr_cast(buffer.get());
	}

	if (GetFileHandle().CanSeek()) {
	        if (!ReadNextBufferSeek(gstate, scan_state, buffer, buffer_index, file_done)) {
	                return false;
	        }
	} else {
	        if (!ReadNextBufferNoSeek(gstate, scan_state, buffer, buffer_index, file_done)) {
	                return false;
	        }
	}
	scan_state.buffer_offset = 0;
	return true;
}

bool BufferedJSONReader::ReadNextBufferSeek(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer, optional_idx &buffer_index, bool &file_done) {
	auto &file_handle = GetFileHandle();

	idx_t request_size = scan_state.buffer_capacity - scan_state.prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_position;
	idx_t read_size;

	{
		lock_guard<mutex> reader_guard(lock);
		if (file_handle.LastReadRequested()) {
			return false;
		}
		if (!file_handle.GetPositionAndSize(read_position, read_size, request_size)) {
			return false; // We weren't able to read
		}
		if (GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
			scan_state.batch_index = gstate.batch_index++;
		}
		buffer_index = GetBufferIndex();
		scan_state.is_last = read_size == 0;
	}
	scan_state.buffer_size = scan_state.prev_buffer_remainder + read_size;

	if (read_size != 0) {
		auto &raw_handle = file_handle.GetHandle();
		// For non-on-disk files, we create a handle per thread: this is faster for e.g. S3Filesystem where throttling
		// per tcp connection can occur meaning that using multiple connections is faster.
		if (!raw_handle.OnDiskFile() && raw_handle.CanSeek()) {
			if (!scan_state.thread_local_filehandle || scan_state.thread_local_filehandle->GetPath() != raw_handle.GetPath()) {
				scan_state.thread_local_filehandle =
					scan_state.fs.OpenFile(raw_handle.GetPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
			}
		} else if (scan_state.thread_local_filehandle) {
			scan_state.thread_local_filehandle = nullptr;
		}
	}

	// Now read the file lock-free!
	file_handle.ReadAtPosition(scan_state.buffer_ptr + scan_state.prev_buffer_remainder, read_size, read_position,
							   file_done, options.type == JSONScanType::SAMPLE,
							   scan_state.thread_local_filehandle);

	return true;

}

bool BufferedJSONReader::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer, optional_idx &buffer_index, bool &file_done) {
	idx_t request_size = scan_state.buffer_capacity - scan_state.prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_size;

	{
		lock_guard<mutex> reader_guard(lock);
		if (!HasFileHandle() || !IsOpen()) {
			return false; // Couldn't read anything
		}
		auto &file_handle = GetFileHandle();
		if (file_handle.LastReadRequested()) {
			return false;
		}
		if (!file_handle.Read(scan_state.buffer_ptr + scan_state.prev_buffer_remainder, read_size, request_size,
							  file_done, options.type == JSONScanType::SAMPLE)) {
			return false; // Couldn't read anything
		}
		if (GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
			scan_state.batch_index = gstate.batch_index++;
		}
		buffer_index = GetBufferIndex();
		scan_state.is_last = read_size == 0;
	}
	scan_state.buffer_size = scan_state.prev_buffer_remainder + read_size;

	return true;

}

} // namespace duckdb
