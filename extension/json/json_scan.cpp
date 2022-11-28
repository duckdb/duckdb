#include "json_scan.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

JSONScanData::JSONScanData(BufferedJSONReaderOptions options) : options(move(options)) {
}

unique_ptr<FunctionData> JSONScanData::Bind(ClientContext &context, TableFunctionBindInput &input) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning JSON files is disabled through configuration");
	}

	BufferedJSONReaderOptions options;
	vector<string> patterns;
	if (input.inputs[0].type().id() == LogicalTypeId::LIST) {
		// list of globs
		for (auto &val : ListValue::GetChildren(input.inputs[0])) {
			patterns.push_back(StringValue::Get(val));
		}
	} else {
		// single glob pattern
		patterns.push_back(StringValue::Get(input.inputs[0]));
	}

	InitializeFilePaths(context, patterns, options.file_paths);

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "ignore_errors") {
			options.ignore_errors = BooleanValue::Get(kv.second);
		} else if (loption == "maximum_object_size") {
			options.maximum_object_size = MaxValue<idx_t>(UIntegerValue::Get(kv.second), options.maximum_object_size);
		}
	}

	auto &info = (JSONScanInfo &)*input.info;
	if (info.forced_format == JSONFormat::AUTO_DETECT) {
		throw NotImplementedException("Auto-detection of JSON format");
	}
	options.format = info.forced_format;
	options.return_json_strings = info.return_json_strings;

	return make_unique<JSONScanData>(options);
}

void JSONScanData::InitializeFilePaths(ClientContext &context, const vector<string> &patterns,
                                       vector<string> &file_paths) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file_pattern : patterns) {
		auto found_files = fs.Glob(file_pattern, context);
		if (found_files.empty()) {
			throw IOException("No files found that match the pattern \"%s\"", file_pattern);
		}
		file_paths.insert(file_paths.end(), found_files.begin(), found_files.end());
	}
}

JSONBufferHandle::JSONBufferHandle(idx_t file_index_p, idx_t readers_p, AllocatedData &&buffer_p)
    : file_index(file_index_p), readers(readers_p), buffer(move(buffer_p)) {
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data)
    : buffer_capacity(bind_data.options.maximum_object_size * 2),
      json_reader(make_unique<BufferedJSONReader>(context, bind_data.options)), batch_index(0),
      allocator(BufferManager::GetBufferManager(context).GetBufferAllocator()) {
	json_reader->OpenJSONFile();
}

unique_ptr<GlobalTableFunctionState> JSONScanGlobalState::Init(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (JSONScanData &)*input.bind_data;
	return make_unique<JSONScanGlobalState>(context, bind_data);
}

idx_t JSONScanGlobalState::MaxThreads() const {
	switch (json_reader->options.format) {
	case JSONFormat::UNSTRUCTURED:
		return 1;
	case JSONFormat::NEWLINE_DELIMITED:
		return json_reader->MaxThreads(buffer_capacity);
	default:
		throw InternalException("Unknown JSON format");
	}
}

JSONScanLocalState::JSONScanLocalState(JSONScanGlobalState &gstate)
    : batch_index(DConstants::INVALID_INDEX), current_buffer_handle(nullptr), buffer_size(0), buffer_offset(0),
      prev_buffer_remainder(0) {
	reconstruct_buffer = gstate.allocator.Allocate(gstate.json_reader->options.maximum_object_size);
	objects.reserve(STANDARD_VECTOR_SIZE);
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	auto &gstate = (JSONScanGlobalState &)*global_state;
	return make_unique<JSONScanLocalState>(gstate);
}

static inline void RemoveWhitespace(JSONLine lines[], const idx_t &count) {
	for (idx_t i = 0; i < count; i++) {
		auto &line = lines[i];
		while (line.size != 0 && std::isspace(line[0])) {
			line.pointer++;
			line.size--;
		}
		while (line.size != 0 && std::isspace(line[line.size - 1])) {
			line.size--;
		}
	}
}

static inline void SkipWhitespace(const char *buffer_ptr, idx_t &buffer_offset, idx_t &buffer_size) {
	for (; buffer_offset != buffer_size; buffer_offset++) {
		if (!std::isspace(buffer_ptr[buffer_offset])) {
			break;
		}
	}
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	idx_t count = 0;
	if (buffer_offset == buffer_size) {
		bool first_read;
		if (!ReadNextBuffer(gstate, first_read)) {
			return 0;
		}

		if (!first_read && gstate.json_reader->options.format == JSONFormat::NEWLINE_DELIMITED) {
			ReconstructFirstObject(gstate);
			count++;
		}
	}

	switch (gstate.json_reader->options.format) {
	case JSONFormat::UNSTRUCTURED:
		ReadUnstructured(count, gstate.json_reader->options.return_json_strings,
		                 gstate.json_reader->options.maximum_object_size);
		break;
	case JSONFormat::NEWLINE_DELIMITED:
		ReadNewlineDelimited(count);
		break;
	default:
		throw InternalException("Unknown JSON format");
	}

	// Remove whitespace around the json objects
	RemoveWhitespace(lines, count);

	// Skip over any remaining whitespace for the next scan
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);

	return count;
}

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate, bool &first_read) {
	auto &buffer_map = gstate.buffer_map;
	AllocatedData buffer;
	if (current_buffer_handle != nullptr && --current_buffer_handle->readers == 0) {
		// Take ownership of the last buffer this thread used and remove entry from map
		lock_guard<mutex> guard(gstate.lock);
		buffer = move(current_buffer_handle->buffer);
		buffer_map.erase(batch_index);
	} else {
		// Allocate a new buffer
		buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
	}
	buffer_ptr = (const char *)buffer.get();

	if (gstate.json_reader->options.format == JSONFormat::UNSTRUCTURED) {
		// Copy last bit of previous buffer
		auto reconstruct_ptr = reconstruct_buffer.get();
		memcpy((void *)buffer_ptr, reconstruct_ptr, prev_buffer_remainder);
	}

	idx_t file_index;
	idx_t next_batch_index;
	idx_t readers;
	auto &file_handle = gstate.json_reader->GetFileHandle(0);
	if (file_handle.CanSeek()) {
		ReadNextBufferSeek(gstate, first_read, file_index, next_batch_index, readers);
	} else {
		ReadNextBufferNoSeek(gstate, first_read, file_index, next_batch_index, readers);
	}

	if (buffer_size == 0) {
		return false;
	}

	// Create an entry and insert it into the map
	batch_index = next_batch_index;
	auto json_buffer_handle = make_unique<JSONBufferHandle>(file_index, readers, move(buffer));
	current_buffer_handle = json_buffer_handle.get();
	{
		lock_guard<mutex> guard(gstate.lock);
		buffer_map.insert(make_pair(batch_index, move(json_buffer_handle)));
	}

	return true;
}

void JSONScanLocalState::ReadNextBufferSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &file_index,
                                            idx_t &next_batch_index, idx_t &readers) {
	auto &options = gstate.json_reader->options;
	auto &format = options.format;
	D_ASSERT(format != JSONFormat::AUTO_DETECT); // should be detected by now

	idx_t request_size = gstate.buffer_capacity;
	if (format == JSONFormat::UNSTRUCTURED) {
		request_size -= prev_buffer_remainder;
		request_size -= YYJSON_PADDING_SIZE;
	}

	idx_t read_position;
	while (true) {
		lock_guard<mutex> guard(gstate.lock);
		file_index = gstate.json_reader->GetFileIndex();
		auto &file_handle = gstate.json_reader->GetFileHandle(file_index);

		buffer_size = file_handle.GetPositionAndSize(read_position, request_size);
		buffer_offset = 0;

		next_batch_index = gstate.batch_index++;
		is_last = file_handle.Remaining() == 0;

		if (buffer_size != 0) {
			// We read something
			break;
		}

		// We didn't read anything
		if (prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", options.file_paths[file_index]);
		}

		if (file_index < options.file_paths.size() - 1) {
			// Open the next file
			gstate.json_reader->OpenJSONFile();
		} else {
			// No more files left
			break;
		}
	}

	if (buffer_size == 0) {
		return;
	}

	if (format == JSONFormat::UNSTRUCTURED) {
		readers = 1;
	} else {
		readers = is_last ? 1 : 2;
	}
	first_read = read_position == 0;

	// Now read the file lock-free!
	auto &file_handle = gstate.json_reader->GetFileHandle(file_index);
	if (format == JSONFormat::UNSTRUCTURED) {
		file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, buffer_size, read_position);
		buffer_size += prev_buffer_remainder;
		memset((void *)(buffer_ptr + buffer_size), 0, 4); // add padding for yyjson insitu
	} else {
		file_handle.ReadAtPosition(buffer_ptr, buffer_size, read_position);
	}
}

void JSONScanLocalState::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &file_index,
                                              idx_t &next_batch_index, idx_t &readers) {
	auto &file_handle = gstate.json_reader->GetFileHandle(file_index);
	auto &options = gstate.json_reader->options;
	auto &format = options.format;

	while (true) {
		idx_t request_size = gstate.buffer_capacity;
		if (format == JSONFormat::UNSTRUCTURED) {
			request_size -= prev_buffer_remainder;
			request_size -= YYJSON_PADDING_SIZE;
		}

		file_index = gstate.json_reader->GetFileIndex();
		next_batch_index = gstate.batch_index++;
		first_read = file_handle.Remaining() == file_handle.FileSize();

		if (format == JSONFormat::UNSTRUCTURED) {
			buffer_size = file_handle.Read(buffer_ptr + prev_buffer_remainder, request_size);
			buffer_size += prev_buffer_remainder;
			memset((void *)(buffer_ptr + buffer_size), 0, 4); // add padding for yyjson insitu
		} else {
			D_ASSERT(format == JSONFormat::NEWLINE_DELIMITED);
			buffer_size = file_handle.Read(buffer_ptr, request_size);
		}

		buffer_offset = 0;
		is_last = buffer_size < request_size;

		if (format == JSONFormat::UNSTRUCTURED) {
			readers = 1;
		} else {
			readers = is_last ? 1 : 2;
		}

		if (buffer_size != 0) {
			// We read something
			break;
		}

		// We didn't read anything
		if (prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", options.file_paths[file_index]);
		}

		if (file_index < options.file_paths.size() - 1) {
			// Open the next file
			gstate.json_reader->OpenJSONFile();
		} else {
			// No more files left
			break;
		}
	}
}

static inline const char *NextNewline(const char *ptr, idx_t size) {
	auto first_n = (const char *)memchr(ptr, '\n', size);
	auto first_r = (const char *)memchr(ptr, '\r', size);

	// if either is nullptr we return the other
	if (first_r == nullptr) {
		return first_n;
	} else if (first_n == nullptr) {
		return first_r;
	}

	// if neither is nullptr we return the first
	if (first_n - ptr < first_r - ptr) {
		return first_n;
	} else {
		return first_r;
	}
}

static inline const char *PreviousNewline(const char *ptr) {
	for (ptr--; true; ptr++) {
		const auto &c = *ptr;
		if (c == '\n' || c == '\r') {
			break;
		}
	}
	return ptr;
}

void JSONScanLocalState::ReconstructFirstObject(JSONScanGlobalState &gstate) {
	D_ASSERT(batch_index != 0);
	D_ASSERT(gstate.json_reader->options.format == JSONFormat::NEWLINE_DELIMITED);
	JSONBufferHandle *previous_buffer_handle;

	// Spinlock until the previous batch index has also read its buffer
	auto &buffer_map = gstate.buffer_map;
	while (true) {
		lock_guard<mutex> guard(gstate.lock);
		auto it = buffer_map.find(batch_index - 1);
		if (it == buffer_map.end()) {
			continue;
		}
		// We found it in the map!
		previous_buffer_handle = it->second.get();
		break;
	}

	// First we find the newline in the previous block
	auto &prev_buffer = previous_buffer_handle->buffer;
	auto prev_buffer_ptr = (const char *)prev_buffer.get() + prev_buffer.GetSize();
	auto part1_ptr = PreviousNewline(prev_buffer_ptr);
	auto part1_size = prev_buffer_ptr - part1_ptr;

	// Now copy the data to our reconstruct buffer
	const auto reconstruct_ptr = reconstruct_buffer.get();
	memcpy(reconstruct_ptr, part1_ptr, part1_size);
	// Now find the newline in the current block
	auto line_end = NextNewline(buffer_ptr, buffer_size);
	if (line_end == nullptr) {
		throw InvalidInputException("maximum_object_size of %llu bytes exceeded (>%llu bytes), is the JSON valid?",
		                            gstate.json_reader->options.maximum_object_size, buffer_size - buffer_offset);
	} else {
		line_end++;
	}
	idx_t part2_size = line_end - buffer_ptr;

	// And copy the remainder of the line to the reconstruct buffer
	memcpy(reconstruct_ptr + part1_size, buffer_ptr, part2_size);
	buffer_offset += part2_size;

	// We copied the object, so we are no longer reading the previous buffer
	if (--previous_buffer_handle->readers == 0) {
		lock_guard<mutex> guard(gstate.lock);
		buffer_map.erase(batch_index - 1);
	}

	lines[0].pointer = (const char *)reconstruct_ptr;
	lines[0].size = part1_size + part2_size;
}

static inline void RestoreParsedString(const char *line_start, const idx_t remaining) {
	// YYJSON replaces some double-quotes with '\0' when parsing insitu
	std::replace((char *)line_start, (char *)line_start + remaining, '\0', '"');
}

void JSONScanLocalState::ReadUnstructured(idx_t &count, const bool return_strings, const idx_t maximum_object_size) {
	objects.clear();
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;

		// Read next JSON doc
		objects.emplace_back(JSONCommon::ReadDocumentFromFileUnsafe((char *)line_start, remaining));
		auto &read_doc = objects.back();

		if (read_doc.IsNull()) {
			if (remaining > maximum_object_size) {
				throw InvalidInputException(
				    "maximum_object_size of %llu bytes exceeded (>%llu bytes), is the JSON valid?", maximum_object_size,
				    remaining);
			}

			RestoreParsedString(line_start, remaining); // Always restore because we will re-parse
			objects.pop_back();
			if (!is_last) {
				// Copy remaining to reconstruct_buffer
				const auto reconstruct_ptr = reconstruct_buffer.get();
				memcpy(reconstruct_ptr, line_start, remaining);
			}

			prev_buffer_remainder = buffer_size - buffer_offset;
			buffer_offset = buffer_size;
			break;
		}
		idx_t line_size = read_doc.ReadSize();

		if (return_strings) {
			RestoreParsedString(line_start, line_size);
		}

		lines[count].pointer = line_start;
		lines[count].size = line_size;
		buffer_offset += line_size;

		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}

	D_ASSERT(count == objects.size());
}

void JSONScanLocalState::ReadNewlineDelimited(idx_t &count) {
	// There is data left in the buffer, scan line-by-line
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;

		// Search for newline
		auto line_end = NextNewline(line_start, remaining);
		if (line_end == nullptr) {
			// We reached the end of the buffer
			if (!is_last || remaining == 0) {
				// Last bit of data belongs to the next batch
				buffer_offset = buffer_size;
				break;
			}
			line_end = line_start + remaining;
		}

		idx_t line_size = line_end - line_start;

		// Create an entry
		lines[count].pointer = line_start;
		lines[count].size = line_size;
		buffer_offset += line_size;
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}
}

idx_t JSONScanLocalState::GetBatchIndex() const {
	return batch_index;
}

} // namespace duckdb
