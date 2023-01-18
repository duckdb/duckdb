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

JSONBufferHandle::JSONBufferHandle(idx_t file_index_p, idx_t readers_p, AllocatedData &&buffer_p, idx_t buffer_size)
    : file_index(file_index_p), readers(readers_p), buffer(move(buffer_p)), buffer_size(buffer_size) {
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

JSONScanLocalState::JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate)
    : json_allocator(BufferAllocator::Get(context)), batch_index(DConstants::INVALID_INDEX),
      current_buffer_handle(nullptr), buffer_size(0), buffer_offset(0), prev_buffer_remainder(0) {
	reconstruct_buffer = gstate.allocator.Allocate(gstate.json_reader->options.maximum_object_size);
	if (gstate.json_reader->options.format == JSONFormat::UNSTRUCTURED) {
		current_buffer_copy = gstate.allocator.Allocate(gstate.buffer_capacity);
		buffer_copy_ptr = (const char *)current_buffer_copy.get();
	}
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	auto &gstate = (JSONScanGlobalState &)*global_state;
	return make_unique<JSONScanLocalState>(context.client, gstate);
}

static inline void SkipWhitespace(const char *buffer_ptr, idx_t &buffer_offset, idx_t &buffer_size) {
	for (; buffer_offset != buffer_size; buffer_offset++) {
		if (!std::isspace(buffer_ptr[buffer_offset])) {
			break;
		}
	}
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	const auto &options = gstate.json_reader->options;
	json_allocator.Reset();

	idx_t count = 0;
	if (buffer_offset == buffer_size) {
		bool first_read;
		if (!ReadNextBuffer(gstate, first_read)) {
			return 0;
		}

		if (!first_read && options.format == JSONFormat::NEWLINE_DELIMITED) {
			ReconstructFirstObject(gstate);
			count++;
		}
	}

	switch (gstate.json_reader->options.format) {
	case JSONFormat::UNSTRUCTURED:
		ReadUnstructured(count, options);
		break;
	case JSONFormat::NEWLINE_DELIMITED:
		ReadNewlineDelimited(count, options);
		break;
	default:
		throw InternalException("Unknown JSON format");
	}

	// Skip over any remaining whitespace for the next scan
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);

	return count;
}

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate, bool &first_read) {
	auto &options = gstate.json_reader->options;
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

	if (options.format == JSONFormat::UNSTRUCTURED) {
		// Copy last bit of previous buffer
		memcpy(buffer.get(), reconstruct_buffer.get(), prev_buffer_remainder);
	}

	idx_t file_index;
	idx_t next_batch_index;
	idx_t readers;
	auto &file_handle = gstate.json_reader->GetFileHandle(0);
	if (file_handle.CanSeek()) {
		ReadNextBufferSeek(gstate, first_read, file_index, next_batch_index);
	} else {
		ReadNextBufferNoSeek(gstate, first_read, file_index, next_batch_index);
	}

	if (buffer_size == 0) {
		return false;
	}

	if (options.format == JSONFormat::UNSTRUCTURED) {
		readers = 1;
	} else {
		readers = is_last ? 1 : 2;
	}

	// Create an entry and insert it into the map
	batch_index = next_batch_index;
	auto json_buffer_handle = make_unique<JSONBufferHandle>(file_index, readers, move(buffer), buffer_size);
	current_buffer_handle = json_buffer_handle.get();
	{
		lock_guard<mutex> guard(gstate.lock);
		buffer_map.insert(make_pair(batch_index, move(json_buffer_handle)));
	}

	buffer_offset = 0;
	prev_buffer_remainder = 0;

	if (options.format == JSONFormat::UNSTRUCTURED) {
		memset((void *)(buffer_ptr + buffer_size), 0, YYJSON_PADDING_SIZE);
		memcpy((void *)buffer_copy_ptr, buffer_ptr, buffer_size + YYJSON_PADDING_SIZE);
	}

	return true;
}

void JSONScanLocalState::ReadNextBufferSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &file_index,
                                            idx_t &next_batch_index) {
	auto &options = gstate.json_reader->options;

	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_position;
	idx_t read_size;
	while (true) {
		lock_guard<mutex> guard(gstate.lock);
		next_batch_index = gstate.batch_index++;

		file_index = gstate.json_reader->GetFileIndex();
		auto &file_handle = gstate.json_reader->GetFileHandle(file_index);

		read_size = file_handle.GetPositionAndSize(read_position, request_size);
		first_read = read_position == 0;
		is_last = file_handle.Remaining() == 0;

		if (read_size != 0) {
			break; // We can read something
		}

		// We didn't read anything - file is done
		if (prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", options.file_paths[file_index]);
		} else if (file_index < options.file_paths.size() - 1) {
			gstate.json_reader->OpenJSONFile(); // Open the next file
		} else {
			break; // No more files left
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
	if (buffer_size == 0) {
		return;
	}

	// Now read the file lock-free!
	auto &file_handle = gstate.json_reader->GetFileHandle(file_index);
	file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, read_size, read_position);
}

void JSONScanLocalState::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &file_index,
                                              idx_t &next_batch_index) {
	auto &options = gstate.json_reader->options;

	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_size;
	while (true) {
		lock_guard<mutex> guard(gstate.lock);
		next_batch_index = gstate.batch_index++;

		file_index = gstate.json_reader->GetFileIndex();
		auto &file_handle = gstate.json_reader->GetFileHandle(file_index);
		first_read = file_handle.Remaining() == file_handle.FileSize();

		read_size = file_handle.Read(buffer_ptr + prev_buffer_remainder, request_size);
		is_last = read_size < request_size;

		if (read_size != 0) {
			break; // We read something
		}

		// We didn't read anything - file is done
		if (prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", options.file_paths[file_index]);
		} else if (file_index < options.file_paths.size() - 1) {
			gstate.json_reader->OpenJSONFile(); // Open the next file
		} else {
			break; // No more files left
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
}

static inline const char *NextNewline(const char *ptr, idx_t size) {
	return (const char *)memchr(ptr, '\n', size);
}

static inline const char *PreviousNewline(const char *ptr) {
	for (ptr--; true; ptr--) {
		const auto &c = *ptr;
		if (c == '\n') {
			break;
		}
	}
	return ptr;
}

static inline void TrimWhitespace(JSONLine &line) {
	while (line.size != 0 && std::isspace(line[0])) {
		line.pointer++;
		line.size--;
	}
	while (line.size != 0 && std::isspace(line[line.size - 1])) {
		line.size--;
	}
}

yyjson_doc *JSONScanLocalState::ParseLine(char *line_start, idx_t line_size, JSONLine &line) {
	// Parse to validate
	auto result =
	    JSONCommon::ReadDocument(line_start, line_size, JSONCommon::READ_FLAG, json_allocator.GetYYJSONAllocator());

	// Set the JSONLine and trim
	line = JSONLine(line_start, line_size);
	TrimWhitespace(line);

	return result;
}

void JSONScanLocalState::ReconstructFirstObject(JSONScanGlobalState &gstate) {
	D_ASSERT(batch_index != 0);
	D_ASSERT(gstate.json_reader->options.format == JSONFormat::NEWLINE_DELIMITED);
	const auto &options = gstate.json_reader->options;
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
	auto prev_buffer_ptr = (const char *)previous_buffer_handle->buffer.get() + previous_buffer_handle->buffer_size;
	auto part1_ptr = PreviousNewline(prev_buffer_ptr);
	auto part1_size = prev_buffer_ptr - part1_ptr;

	// Now copy the data to our reconstruct buffer
	const auto reconstruct_ptr = reconstruct_buffer.get();
	memcpy(reconstruct_ptr, part1_ptr, part1_size);
	// Now find the newline in the current block
	auto line_end = NextNewline(buffer_ptr, buffer_size);
	if (line_end == nullptr) {
		throw InvalidInputException("maximum_object_size of %llu bytes exceeded (>%llu bytes), is the JSON valid?",
		                            options.maximum_object_size, buffer_size - buffer_offset);
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

	objects[0] = ParseLine((char *)reconstruct_ptr, part1_size + part2_size, lines[0]);
}

void JSONScanLocalState::ReadUnstructured(idx_t &count, const BufferedJSONReaderOptions &options) {
	yyjson_read_err error;
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		const auto obj_start = buffer_ptr + buffer_offset;
		const auto obj_copy_start = buffer_copy_ptr + buffer_offset;

		idx_t remaining = buffer_size - buffer_offset;
		if (remaining == 0) {
			break;
		}

		// Read next JSON doc
		auto read_doc = JSONCommon::ReadDocumentUnsafe((char *)obj_start, remaining, JSONCommon::STOP_READ_FLAG,
		                                               json_allocator.GetYYJSONAllocator(), &error);
		if (error.code == YYJSON_READ_SUCCESS) {
			idx_t line_size = yyjson_doc_get_read_size(read_doc);
			lines[count] = JSONLine(obj_copy_start, line_size);
			TrimWhitespace(lines[count]);

			buffer_offset += line_size;
			SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
		} else if (error.pos > options.maximum_object_size) {
			JSONCommon::ThrowParseError(obj_copy_start, remaining, error,
			                            "Have you tried increasing maximum_object_size?");
		} else if (error.code == YYJSON_READ_ERROR_UNEXPECTED_END && !is_last) {
			// Copy remaining to reconstruct_buffer
			const auto reconstruct_ptr = reconstruct_buffer.get();
			memcpy(reconstruct_ptr, obj_copy_start, remaining);
			prev_buffer_remainder = remaining;
			buffer_offset = buffer_size;
			break;
		} else {
			JSONCommon::ThrowParseError(obj_copy_start, remaining, error);
		}
		objects[count] = read_doc;
	}
}

void JSONScanLocalState::ReadNewlineDelimited(idx_t &count, const BufferedJSONReaderOptions &options) {
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;
		if (remaining == 0) {
			break;
		}

		// Search for newline
		auto line_end = NextNewline(line_start, remaining);

		if (line_end == nullptr) {
			// We reached the end of the buffer
			if (!is_last) {
				// Last bit of data belongs to the next batch
				buffer_offset = buffer_size;
				break;
			}
			line_end = line_start + remaining;
		}
		idx_t line_size = line_end - line_start;

		objects[count] = ParseLine((char *)line_start, line_size, lines[count]);

		buffer_offset += line_size;
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}
}

idx_t JSONScanLocalState::GetBatchIndex() const {
	return batch_index;
}

} // namespace duckdb
