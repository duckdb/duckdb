#include "json_scan.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

JSONScanData::JSONScanData() {
}

unique_ptr<FunctionData> JSONScanData::Bind(ClientContext &context, TableFunctionBindInput &input) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning JSON files is disabled through configuration");
	}

	auto result = make_unique<JSONScanData>();
	auto &options = result->options;

	auto &info = (JSONScanInfo &)*input.info;
	if (info.forced_format == JSONFormat::AUTO_DETECT) {
		throw NotImplementedException("Auto-detection of JSON format");
	}
	options.format = info.forced_format;
	result->return_json_strings = info.return_json_strings;

	vector<string> patterns;
	if (input.inputs[0].type().id() == LogicalTypeId::LIST) { // list of globs
		for (auto &val : ListValue::GetChildren(input.inputs[0])) {
			patterns.push_back(StringValue::Get(val));
		}
	} else { // single glob pattern
		patterns.push_back(StringValue::Get(input.inputs[0]));
	}
	InitializeFilePaths(context, patterns, result->file_paths);

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "ignore_errors") {
			result->ignore_errors = BooleanValue::Get(kv.second);
		} else if (loption == "maximum_object_size") {
			result->maximum_object_size = MaxValue<idx_t>(UIntegerValue::Get(kv.second), result->maximum_object_size);
		} else if (loption == "format") {
			auto format = StringUtil::Lower(StringValue::Get(kv.second));
			if (format == "auto") {
				options.format = JSONFormat::AUTO_DETECT;
			} else if (format == "unstructured") {
				options.format = JSONFormat::UNSTRUCTURED;
			} else if (format == "newline_delimited") {
				options.format = JSONFormat::NEWLINE_DELIMITED;
			} else {
				throw InvalidInputException("format must be one of ['auto', 'unstructured', 'newline_delimited']");
			}
		}
	}

	if (result->ignore_errors && options.format == JSONFormat::UNSTRUCTURED) {
		throw InvalidInputException("Cannot ignore errors with unstructured format");
	}

	return std::move(result);
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

void JSONScanData::Serialize(FieldWriter &writer) {
	options.Serialize(writer);
	writer.WriteList<string>(file_paths);
	writer.WriteField<bool>(ignore_errors);
	writer.WriteField<idx_t>(maximum_object_size);
	writer.WriteField<bool>(return_json_strings);
}

void JSONScanData::Deserialize(FieldReader &reader) {
	options.Deserialize(reader);
	file_paths = reader.ReadRequiredList<string>();
	ignore_errors = reader.ReadRequired<bool>();
	maximum_object_size = reader.ReadRequired<idx_t>();
	return_json_strings = reader.ReadRequired<bool>();
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data_p)
    : bind_data(bind_data_p), allocator(BufferManager::GetBufferManager(context).GetBufferAllocator()),
      buffer_capacity(bind_data.maximum_object_size * 2), file_index(0), batch_index(0),
      system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()) {
	json_readers.reserve(bind_data.file_paths.size());
	for (idx_t i = 0; i < bind_data_p.file_paths.size(); i++) {
		json_readers.push_back(make_unique<BufferedJSONReader>(context, bind_data.options, i, bind_data.file_paths[i]));
	}
}

unique_ptr<GlobalTableFunctionState> JSONScanGlobalState::Init(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (JSONScanData &)*input.bind_data;
	return make_unique<JSONScanGlobalState>(context, bind_data);
}

idx_t JSONScanGlobalState::MaxThreads() const {
	return system_threads;
}

JSONScanLocalState::JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate)
    : batch_index(DConstants::INVALID_INDEX), json_allocator(BufferAllocator::Get(context)), current_reader(nullptr),
      current_buffer_handle(nullptr), buffer_size(0), buffer_offset(0), prev_buffer_remainder(0) {

	// Buffer to reconstruct JSON objects when they cross a buffer boundary
	reconstruct_buffer = gstate.allocator.Allocate(gstate.bind_data.maximum_object_size);

	// This is needed for JSONFormat::UNSTRUCTURED, to make use of YYJSON_READ_INSITU
	current_buffer_copy = gstate.allocator.Allocate(gstate.buffer_capacity);
	buffer_copy_ptr = (const char *)current_buffer_copy.get();
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	auto &gstate = (JSONScanGlobalState &)*global_state;
	return make_unique<JSONScanLocalState>(context.client, gstate);
}

static inline void SkipWhitespace(const char *buffer_ptr, idx_t &buffer_offset, idx_t &buffer_size) {
	for (; buffer_offset != buffer_size; buffer_offset++) {
		if (!StringUtil::CharacterIsSpace(buffer_ptr[buffer_offset])) {
			break;
		}
	}
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	json_allocator.Reset();

	idx_t count = 0;
	if (buffer_offset == buffer_size) {
		bool first_read;
		if (!ReadNextBuffer(gstate, first_read)) {
			return 0;
		}

		if (!first_read && current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED) {
			ReconstructFirstObject(gstate);
			count++;
		}
	}

	auto &options = current_reader->GetOptions();
	switch (options.format) {
	case JSONFormat::UNSTRUCTURED:
		ReadUnstructured(count);
		break;
	case JSONFormat::NEWLINE_DELIMITED:
		ReadNewlineDelimited(count, gstate.bind_data.ignore_errors);
		break;
	default:
		throw InternalException("Unknown JSON format");
	}

	// Skip over any remaining whitespace for the next scan
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);

	return count;
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
	while (line.size != 0 && StringUtil::CharacterIsSpace(line[0])) {
		line.pointer++;
		line.size--;
	}
	while (line.size != 0 && StringUtil::CharacterIsSpace(line[line.size - 1])) {
		line.size--;
	}
}

yyjson_doc *JSONScanLocalState::ParseLine(char *line_start, idx_t line_size, JSONLine &line,
                                          const bool &ignore_errors) {
	// Parse to validate TODO: This is the only place we can maybe parse INSITU (if not returning strings)
	yyjson_doc *result;
	if (ignore_errors) {
		result = JSONCommon::ReadDocumentUnsafe(line_start, line_size, JSONCommon::READ_FLAG,
		                                        json_allocator.GetYYJSONAllocator());
	} else {
		result =
		    JSONCommon::ReadDocument(line_start, line_size, JSONCommon::READ_FLAG, json_allocator.GetYYJSONAllocator());
	}

	if (result) {
		// Set the JSONLine and trim
		line = JSONLine(line_start, line_size);
		TrimWhitespace(line);
	}

	return result;
}

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate, bool &first_read) {
	AllocatedData buffer;
	if (current_buffer_handle && --current_buffer_handle->readers == 0) {
		D_ASSERT(current_reader);
		// Take ownership of the last buffer this thread used and remove entry from map
		buffer = current_reader->RemoveBuffer(current_buffer_handle->buffer_index);
	} else {
		// Allocate a new buffer
		buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
	}
	buffer_ptr = (const char *)buffer.get();

	if (current_reader && current_reader->GetOptions().format == JSONFormat::UNSTRUCTURED) {
		// Copy last bit of previous buffer
		memcpy(buffer.get(), reconstruct_buffer.get(), prev_buffer_remainder);
	}

	idx_t buffer_index;
	while (true) {
		if (current_reader) {
			if (current_reader->GetFileHandle().CanSeek()) {
				ReadNextBufferSeek(gstate, first_read, buffer_index);
			} else {
				ReadNextBufferNoSeek(gstate, first_read, buffer_index);
			}
			if (buffer_size != 0) {
				if (current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED) {
					lock_guard<mutex> guard(gstate.lock);
					batch_index = gstate.batch_index++;
				}
				break; // We read something!
			}
		}

		// No reader, or exhausted current reader
		lock_guard<mutex> guard(gstate.lock);
		D_ASSERT(gstate.file_index <= gstate.json_readers.size());
		if (gstate.file_index == gstate.json_readers.size()) {
			return false; // No more files left
		}
		if (current_reader && current_reader == gstate.json_readers[gstate.file_index].get() &&
		    current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED) {
			// We had a reader, but we didn't read anything, move to the next file
			gstate.file_index++;
		}
		// Check again since we may have just updated
		if (gstate.file_index == gstate.json_readers.size()) {
			return false; // No more files left
		}

		// Try the next reader
		current_reader = gstate.json_readers[gstate.file_index].get();
		if (current_reader->IsOpen()) {
			continue; // It's open, this thread joins the scan
		}

		// Unopened file
		auto &options = current_reader->GetOptions();
		current_reader->OpenJSONFile();
		batch_index = gstate.batch_index++;
		if (options.format == JSONFormat::UNSTRUCTURED) {
			gstate.file_index++; // UNSTRUCTURED necessitates single-threaded read
		}
		if (options.format != JSONFormat::AUTO_DETECT) {
			continue; // Re-enter loop to proceed reading
		}

		// We have to detect whether it's UNSTRUCTURED/NEWLINE_DELIMITED - hold the gstate lock while we do this
		if (current_reader->GetFileHandle().CanSeek()) {
			ReadNextBufferSeek(gstate, first_read, buffer_index);
		} else {
			ReadNextBufferNoSeek(gstate, first_read, buffer_index);
		}

		if (buffer_size == 0) {
			gstate.file_index++; // Empty file, move to the next one
			continue;
		}

		auto line_end = NextNewline(buffer_ptr, buffer_size);
		if (line_end == nullptr) {
			options.format = JSONFormat::UNSTRUCTURED; // No newlines in buffer at all
			gstate.file_index++;                       // UNSTRUCTURED necessitates single-threaded read
			break;
		}
		idx_t line_size = line_end - buffer_ptr;

		yyjson_read_err error;
		JSONCommon::ReadDocumentUnsafe((char *)buffer_ptr, line_size, JSONCommon::READ_FLAG,
		                               json_allocator.GetYYJSONAllocator(), &error);
		// Detected format depends on whether we can successfully read the first line
		if (error.code == YYJSON_READ_SUCCESS) {
			options.format = JSONFormat::NEWLINE_DELIMITED;
		} else {
			options.format = JSONFormat::UNSTRUCTURED;
			gstate.file_index++; // UNSTRUCTURED necessitates single-threaded read
		}
		break;
	}
	D_ASSERT(buffer_size != 0); // We should have read something if we got here

	idx_t readers;
	if (current_reader->GetOptions().format == JSONFormat::UNSTRUCTURED) {
		readers = 1;
	} else {
		readers = is_last ? 1 : 2;
	}

	// Create an entry and insert it into the map
	auto json_buffer_handle = make_unique<JSONBufferHandle>(buffer_index, readers, move(buffer), buffer_size);
	current_buffer_handle = json_buffer_handle.get();
	current_reader->InsertBuffer(buffer_index, std::move(json_buffer_handle));

	buffer_offset = 0;
	prev_buffer_remainder = 0;

	if (current_reader->GetOptions().format == JSONFormat::UNSTRUCTURED) {
		memset((void *)(buffer_ptr + buffer_size), 0, YYJSON_PADDING_SIZE);
		memcpy((void *)buffer_copy_ptr, buffer_ptr, buffer_size + YYJSON_PADDING_SIZE);
	}

	return true;
}

void JSONScanLocalState::ReadNextBufferSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &buffer_index) {
	auto &file_handle = current_reader->GetFileHandle();

	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_position;
	idx_t read_size;

	{
		lock_guard<mutex> guard(current_reader->lock);
		buffer_index = current_reader->GetBufferIndex();

		read_size = file_handle.GetPositionAndSize(read_position, request_size);
		first_read = read_position == 0;
		is_last = file_handle.Remaining() == 0;

		if (!gstate.bind_data.ignore_errors && read_size == 0 && prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", current_reader->file_path);
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
	if (buffer_size == 0) {
		return;
	}

	// Now read the file lock-free!
	file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, read_size, read_position);
}

void JSONScanLocalState::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &buffer_index) {
	auto &file_handle = current_reader->GetFileHandle();

	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_size;
	{
		lock_guard<mutex> guard(gstate.lock);
		buffer_index = current_reader->GetBufferIndex();

		first_read = file_handle.Remaining() == file_handle.FileSize();
		read_size = file_handle.Read(buffer_ptr + prev_buffer_remainder, request_size);
		is_last = read_size < request_size;

		if (!gstate.bind_data.ignore_errors && read_size == 0 && prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", current_reader->file_path);
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
}

void JSONScanLocalState::ReconstructFirstObject(JSONScanGlobalState &gstate) {
	D_ASSERT(current_buffer_handle->buffer_index != 0);
	D_ASSERT(current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED);

	// Spinlock until the previous batch index has also read its buffer
	JSONBufferHandle *previous_buffer_handle = nullptr;
	while (!previous_buffer_handle) {
		previous_buffer_handle = current_reader->GetBuffer(current_buffer_handle->buffer_index - 1);
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
	if (line_end == nullptr) { // TODO I don't think we can ignore this even with ignore_errors ...
		throw InvalidInputException("maximum_object_size of %llu bytes exceeded (>%llu bytes), is the JSON valid?",
		                            gstate.bind_data.maximum_object_size, buffer_size - buffer_offset);
	} else {
		line_end++;
	}
	idx_t part2_size = line_end - buffer_ptr;

	// And copy the remainder of the line to the reconstruct buffer
	memcpy(reconstruct_ptr + part1_size, buffer_ptr, part2_size);
	buffer_offset += part2_size;

	// We copied the object, so we are no longer reading the previous buffer
	if (--previous_buffer_handle->readers == 0) {
		current_reader->RemoveBuffer(current_buffer_handle->buffer_index - 1);
	}

	objects[0] = ParseLine((char *)reconstruct_ptr, part1_size + part2_size, lines[0], gstate.bind_data.ignore_errors);
}

void JSONScanLocalState::ReadUnstructured(idx_t &count) {
	const auto max_obj_size = reconstruct_buffer.GetSize();
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
		} else if (error.pos > max_obj_size) {
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

void JSONScanLocalState::ReadNewlineDelimited(idx_t &count, const bool &ignore_errors) {
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

		objects[count] = ParseLine((char *)line_start, line_size, lines[count], ignore_errors);

		buffer_offset += line_size;
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}
}

idx_t JSONScanLocalState::GetBatchIndex() const {
	return batch_index;
}

} // namespace duckdb
