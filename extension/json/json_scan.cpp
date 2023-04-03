#include "json_scan.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/multi_file_reader.hpp"

namespace duckdb {

JSONScanData::JSONScanData() {
}

unique_ptr<FunctionData> JSONScanData::Bind(ClientContext &context, TableFunctionBindInput &input) {
	auto result = make_uniq<JSONScanData>();
	auto &options = result->options;

	auto &info = (JSONScanInfo &)*input.info;
	result->type = info.type;
	options.format = info.format;
	result->record_type = info.record_type;
	result->auto_detect = info.auto_detect;
	result->file_paths = MultiFileReader::GetFileList(context, input.inputs[0], "JSON");

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "ignore_errors") {
			result->ignore_errors = BooleanValue::Get(kv.second);
		} else if (loption == "maximum_object_size") {
			result->maximum_object_size = MaxValue<idx_t>(UIntegerValue::Get(kv.second), result->maximum_object_size);
		} else if (loption == "lines") {
			auto format = StringUtil::Lower(StringValue::Get(kv.second));
			if (format == "auto") {
				options.format = JSONFormat::AUTO_DETECT;
			} else if (format == "false") {
				options.format = JSONFormat::UNSTRUCTURED;
			} else if (format == "true") {
				options.format = JSONFormat::NEWLINE_DELIMITED;
			} else {
				throw BinderException("\"lines\" must be one of ['auto', 'true', 'false']");
			}
		} else if (loption == "compression") {
			auto compression = StringUtil::Lower(StringValue::Get(kv.second));
			if (compression == "none") {
				options.compression = FileCompressionType::UNCOMPRESSED;
			} else if (compression == "gzip") {
				options.compression = FileCompressionType::GZIP;
			} else if (compression == "zstd") {
				options.compression = FileCompressionType::ZSTD;
			} else if (compression == "auto") {
				options.compression = FileCompressionType::AUTO_DETECT;
			} else {
				throw BinderException("compression must be one of ['none', 'gzip', 'zstd', 'auto']");
			}
		}
	}

	return std::move(result);
}

void JSONScanData::InitializeFormats() {
	// Set defaults for date/timestamp formats if we need to
	if (!auto_detect && date_format.empty()) {
		date_format = "%Y-%m-%d";
	}
	if (!auto_detect && timestamp_format.empty()) {
		timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ";
	}

	// Initialize date_format_map if anything was specified
	if (!date_format.empty()) {
		date_format_map.AddFormat(LogicalTypeId::DATE, date_format);
	}
	if (!timestamp_format.empty()) {
		date_format_map.AddFormat(LogicalTypeId::TIMESTAMP, timestamp_format);
	}

	if (auto_detect) {
		static const unordered_map<LogicalTypeId, vector<const char *>, LogicalTypeIdHash> FORMAT_TEMPLATES = {
		    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
		    {LogicalTypeId::TIMESTAMP,
		     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
		      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"}},
		};

		// Populate possible date/timestamp formats, assume this is consistent across columns
		for (auto &kv : FORMAT_TEMPLATES) {
			const auto &type = kv.first;
			if (date_format_map.HasFormats(type)) {
				continue; // Already populated
			}
			const auto &format_strings = kv.second;
			for (auto &format_string : format_strings) {
				date_format_map.AddFormat(type, format_string);
			}
		}
	}
}

void JSONScanData::Serialize(FieldWriter &writer) {
	writer.WriteField<JSONScanType>(type);
	options.Serialize(writer);
	writer.WriteList<string>(file_paths);
	writer.WriteField<bool>(ignore_errors);
	writer.WriteField<idx_t>(maximum_object_size);
	transform_options.Serialize(writer);
	writer.WriteField<bool>(auto_detect);
	writer.WriteField<idx_t>(sample_size);
	writer.WriteList<string>(names);
	writer.WriteList<idx_t>(valid_cols);
	writer.WriteField<idx_t>(max_depth);
	writer.WriteField<JSONRecordType>(record_type);
	if (!date_format.empty()) {
		writer.WriteString(date_format);
	} else {
		writer.WriteString(date_format_map.GetFormat(LogicalTypeId::DATE).format_specifier);
	}
	if (!timestamp_format.empty()) {
		writer.WriteString(timestamp_format);
	} else {
		writer.WriteString(date_format_map.GetFormat(LogicalTypeId::TIMESTAMP).format_specifier);
	}
}

void JSONScanData::Deserialize(FieldReader &reader) {
	type = reader.ReadRequired<JSONScanType>();
	options.Deserialize(reader);
	file_paths = reader.ReadRequiredList<string>();
	ignore_errors = reader.ReadRequired<bool>();
	maximum_object_size = reader.ReadRequired<idx_t>();
	transform_options.Deserialize(reader);
	auto_detect = reader.ReadRequired<bool>();
	sample_size = reader.ReadRequired<idx_t>();
	names = reader.ReadRequiredList<string>();
	valid_cols = reader.ReadRequiredList<idx_t>();
	max_depth = reader.ReadRequired<idx_t>();
	record_type = reader.ReadRequired<JSONRecordType>();
	date_format = reader.ReadRequired<string>();
	timestamp_format = reader.ReadRequired<string>();

	InitializeFormats();
	transform_options.date_format_map = &date_format_map;
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data_p)
    : bind_data(bind_data_p), allocator(BufferManager::GetBufferManager(context).GetBufferAllocator()),
      buffer_capacity(bind_data.maximum_object_size * 2), file_index(0), batch_index(0),
      system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()) {
	if (bind_data.stored_readers.empty()) {
		json_readers.reserve(bind_data.file_paths.size());
		for (idx_t i = 0; i < bind_data.file_paths.size(); i++) {
			json_readers.push_back(make_uniq<BufferedJSONReader>(context, bind_data.options, bind_data.file_paths[i]));
		}
	} else {
		json_readers = std::move(bind_data.stored_readers);
	}
}

JSONScanLocalState::JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate)
    : scan_count(0), array_idx(0), array_offset(0), batch_index(DConstants::INVALID_INDEX), bind_data(gstate.bind_data),
      json_allocator(BufferAllocator::Get(context)), current_reader(nullptr), current_buffer_handle(nullptr),
      is_last(false), buffer_size(0), buffer_offset(0), prev_buffer_remainder(0) {

	// Buffer to reconstruct JSON values when they cross a buffer boundary
	reconstruct_buffer = gstate.allocator.Allocate(gstate.bind_data.maximum_object_size + YYJSON_PADDING_SIZE);

	// This is needed for JSONFormat::UNSTRUCTURED, to make use of YYJSON_READ_INSITU
	current_buffer_copy = gstate.allocator.Allocate(gstate.buffer_capacity);
	buffer_copy_ptr = (const char *)current_buffer_copy.get();
}

JSONGlobalTableFunctionState::JSONGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input)
    : state(context, (JSONScanData &)*input.bind_data) {
}

unique_ptr<GlobalTableFunctionState> JSONGlobalTableFunctionState::Init(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &bind_data = (JSONScanData &)*input.bind_data;
	auto result = make_uniq<JSONGlobalTableFunctionState>(context, input);

	// Perform projection pushdown
	if (bind_data.type == JSONScanType::READ_JSON) {
		D_ASSERT(input.column_ids.size() <= bind_data.names.size()); // Can't project to have more columns
		vector<string> names;
		names.reserve(input.column_ids.size());
		for (idx_t i = 0; i < input.column_ids.size(); i++) {
			const auto &id = input.column_ids[i];
			if (IsRowIdColumnId(id)) {
				continue;
			}
			names.push_back(std::move(bind_data.names[id]));
			bind_data.valid_cols.push_back(i);
		}
		if (names.size() < bind_data.names.size()) {
			// If we are auto-detecting, but don't need all columns present in the file,
			// then we don't need to throw an error if we encounter an unseen column
			bind_data.transform_options.error_unknown_key = false;
		}
		bind_data.names = std::move(names);
	}
	return std::move(result);
}

idx_t JSONGlobalTableFunctionState::MaxThreads() const {
	auto &bind_data = state.bind_data;

	auto num_files = bind_data.file_paths.size();
	idx_t readers_per_file;
	if (bind_data.options.format == JSONFormat::UNSTRUCTURED) {
		// Unstructured necessitates single thread
		readers_per_file = 1;
	} else if (!state.json_readers.empty() && state.json_readers[0]->IsOpen()) {
		auto &reader = *state.json_readers[0];
		const auto &options = reader.GetOptions();
		if (options.format == JSONFormat::UNSTRUCTURED || options.compression != FileCompressionType::UNCOMPRESSED) {
			// Auto-detected unstructured - same story, compression also really limits parallelism
			readers_per_file = 1;
		} else {
			return state.system_threads;
		}
	} else {
		return state.system_threads;
	}
	return num_files * readers_per_file;
}

JSONLocalTableFunctionState::JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate)
    : state(context, gstate) {
}

unique_ptr<LocalTableFunctionState> JSONLocalTableFunctionState::Init(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state) {
	auto &gstate = (JSONGlobalTableFunctionState &)*global_state;
	auto result = make_uniq<JSONLocalTableFunctionState>(context.client, gstate.state);

	// Copy the transform options / date format map because we need to do thread-local stuff
	result->state.date_format_map = gstate.state.bind_data.date_format_map;
	result->state.transform_options = gstate.state.bind_data.transform_options;
	result->state.transform_options.date_format_map = &result->state.date_format_map;

	return std::move(result);
}

idx_t JSONLocalTableFunctionState::GetBatchIndex() const {
	return state.batch_index;
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

	if ((gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_RECORDS ||
	     gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_JSON) &&
	    array_idx < scan_count) {
		return GetObjectsFromArray(gstate);
	}

	idx_t count = 0;
	if (buffer_offset == buffer_size) {
		if (!ReadNextBuffer(gstate)) {
			return 0;
		}
		if (current_buffer_handle->buffer_index != 0 &&
		    current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED) {
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
		ReadNewlineDelimited(count);
		break;
	default:
		throw InternalException("Unknown JSON format");
	}
	scan_count = count;

	// Skip over any remaining whitespace for the next scan
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);

	if (gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_RECORDS ||
	    gstate.bind_data.record_type == JSONRecordType::ARRAY_OF_JSON) {
		array_idx = 0;
		array_offset = 0;
		return GetObjectsFromArray(gstate);
	}

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

yyjson_val *JSONScanLocalState::ParseLine(char *line_start, idx_t line_size, idx_t remaining, JSONLine &line) {
	yyjson_doc *doc;
	if (bind_data.ignore_errors) {
		doc = JSONCommon::ReadDocumentUnsafe(line_start, line_size, JSONCommon::READ_FLAG,
		                                     json_allocator.GetYYJSONAllocator());
	} else {
		yyjson_read_err err;
		if (bind_data.type != JSONScanType::READ_JSON_OBJECTS) {
			// Optimization: if we don't ignore errors, and don't need to return strings, we can parse INSITU
			doc = JSONCommon::ReadDocumentUnsafe(line_start, remaining, JSONCommon::STOP_READ_FLAG,
			                                     json_allocator.GetYYJSONAllocator(), &err);
			idx_t read_size = yyjson_doc_get_read_size(doc);
			if (read_size > line_size) {
				err.pos = line_size;
				err.code = YYJSON_READ_ERROR_UNEXPECTED_END;
				err.msg = "unexpected end of data";
			} else if (read_size < line_size) {
				idx_t diff = line_size - read_size;
				char *ptr = line_start + read_size;
				for (idx_t i = 0; i < diff; i++) {
					if (!StringUtil::CharacterIsSpace(ptr[i])) {
						err.pos = read_size;
						err.code = YYJSON_READ_ERROR_UNEXPECTED_CONTENT;
						err.msg = "unexpected content after document";
					}
				}
			}
		} else {
			doc = JSONCommon::ReadDocumentUnsafe(line_start, line_size, JSONCommon::READ_FLAG,
			                                     json_allocator.GetYYJSONAllocator(), &err);
		}
		if (err.code != YYJSON_READ_SUCCESS) {
			current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, err);
		}
	}
	lines_or_objects_in_buffer++;

	if (doc) {
		// Set the JSONLine and trim
		line = JSONLine(line_start, line_size);
		TrimWhitespace(line);
		return doc->root;
	} else {
		return nullptr;
	}
}

idx_t JSONScanLocalState::GetObjectsFromArray(JSONScanGlobalState &gstate) {
	idx_t arr_count = 0;

	size_t idx, max;
	yyjson_val *val;
	for (; array_idx < scan_count; array_idx++, array_offset = 0) {
		auto &value = values[array_idx];
		if (!value) {
			continue;
		}
		if (unsafe_yyjson_is_arr(value)) {
			yyjson_arr_foreach(value, idx, max, val) {
				if (idx < array_offset) {
					continue;
				}
				array_values[arr_count++] = val;
				if (arr_count == STANDARD_VECTOR_SIZE) {
					break;
				}
			}
			array_offset = idx + 1;
			if (arr_count == STANDARD_VECTOR_SIZE) {
				break;
			}
		} else if (!gstate.bind_data.ignore_errors) {
			ThrowTransformError(
			    array_idx,
			    StringUtil::Format("Expected JSON ARRAY but got %s: %s\nTry setting json_format to 'records'",
			                       JSONCommon::ValTypeToString(value), JSONCommon::ValToString(value, 50)));
		}
	}
	return arr_count;
}

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate) {
	if (current_reader) {
		D_ASSERT(current_buffer_handle);
		current_reader->SetBufferLineOrObjectCount(current_buffer_handle->buffer_index, lines_or_objects_in_buffer);
		if (is_last && gstate.bind_data.type != JSONScanType::SAMPLE) {
			// Close files that are done if we're not sampling
			current_reader->CloseJSONFile();
		}
	}

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
			ReadNextBuffer(gstate, buffer_index);
			if (buffer_size != 0) {
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
		auto &options = current_reader->GetOptions();
		if (current_reader->IsOpen()) {
			if (options.format == JSONFormat::UNSTRUCTURED ||
			    (options.compression != FileCompressionType::UNCOMPRESSED &&
			     gstate.file_index < gstate.json_readers.size())) {
				// Can only be open from schema detection
				batch_index = gstate.batch_index++;
				gstate.file_index++;
			}
			continue; // It's open, this thread joins the scan
		}

		// Unopened file
		current_reader->OpenJSONFile();
		batch_index = gstate.batch_index++;
		if (options.format == JSONFormat::UNSTRUCTURED || (options.format == JSONFormat::NEWLINE_DELIMITED &&
		                                                   options.compression != FileCompressionType::UNCOMPRESSED &&
		                                                   gstate.file_index < gstate.json_readers.size())) {
			gstate.file_index++; // UNSTRUCTURED necessitates single-threaded read
		}
		if (options.format != JSONFormat::AUTO_DETECT) {
			continue; // Re-enter loop to proceed reading
		}

		// We have to detect whether it's UNSTRUCTURED/NEWLINE_DELIMITED - hold the gstate lock while we do this
		ReadNextBuffer(gstate, buffer_index);
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

		// Optimization: decompression limits parallelism quite a bit
		if (options.compression != FileCompressionType::UNCOMPRESSED &&
		    gstate.file_index < gstate.json_readers.size()) {
			gstate.file_index++;
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
	auto json_buffer_handle = make_uniq<JSONBufferHandle>(buffer_index, readers, std::move(buffer), buffer_size);
	current_buffer_handle = json_buffer_handle.get();
	current_reader->InsertBuffer(buffer_index, std::move(json_buffer_handle));

	buffer_offset = 0;
	prev_buffer_remainder = 0;
	lines_or_objects_in_buffer = 0;

	memset((void *)(buffer_ptr + buffer_size), 0, YYJSON_PADDING_SIZE);
	if (current_reader->GetOptions().format == JSONFormat::UNSTRUCTURED) {
		memcpy((void *)buffer_copy_ptr, buffer_ptr, buffer_size + YYJSON_PADDING_SIZE);
	}

	return true;
}

void JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate, idx_t &buffer_index) {
	if (current_reader->GetFileHandle().CanSeek()) {
		ReadNextBufferSeek(gstate, buffer_index);
	} else {
		ReadNextBufferNoSeek(gstate, buffer_index);
	}
}

void JSONScanLocalState::ReadNextBufferSeek(JSONScanGlobalState &gstate, idx_t &buffer_index) {
	auto &file_handle = current_reader->GetFileHandle();

	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_position;
	idx_t read_size;

	{
		lock_guard<mutex> reader_guard(current_reader->lock);
		buffer_index = current_reader->GetBufferIndex();

		read_size = file_handle.GetPositionAndSize(read_position, request_size);
		is_last = file_handle.Remaining() == 0;

		if (!gstate.bind_data.ignore_errors && read_size == 0 && prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", current_reader->file_path);
		}

		if (current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED) {
			batch_index = gstate.batch_index++;
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
	if (buffer_size == 0) {
		current_reader->SetBufferLineOrObjectCount(buffer_index, 0);
		return;
	}

	// Now read the file lock-free!
	file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, read_size, read_position,
	                           gstate.bind_data.type == JSONScanType::SAMPLE);
}

void JSONScanLocalState::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, idx_t &buffer_index) {
	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_size;
	{
		lock_guard<mutex> reader_guard(current_reader->lock);
		buffer_index = current_reader->GetBufferIndex();

		if (current_reader->IsOpen()) {
			read_size = current_reader->GetFileHandle().Read(buffer_ptr + prev_buffer_remainder, request_size,
			                                                 gstate.bind_data.type == JSONScanType::SAMPLE);
		} else {
			read_size = 0;
		}
		is_last = read_size < request_size;

		if (!gstate.bind_data.ignore_errors && read_size == 0 && prev_buffer_remainder != 0) {
			throw InvalidInputException("Invalid JSON detected at the end of file %s", current_reader->file_path);
		}

		if (current_reader->GetOptions().format == JSONFormat::NEWLINE_DELIMITED) {
			batch_index = gstate.batch_index++;
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
	if (buffer_size == 0) {
		current_reader->SetBufferLineOrObjectCount(buffer_index, 0);
		return;
	}
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
		                            bind_data.maximum_object_size, buffer_size - buffer_offset);
	} else {
		line_end++;
	}
	idx_t part2_size = line_end - buffer_ptr;

	idx_t line_size = part1_size + part2_size;
	if (line_size > bind_data.maximum_object_size) {
		throw InvalidInputException("maximum_object_size of %llu bytes exceeded (%llu bytes), is the JSON valid?",
		                            bind_data.maximum_object_size, line_size);
	}

	// And copy the remainder of the line to the reconstruct buffer
	memcpy(reconstruct_ptr + part1_size, buffer_ptr, part2_size);
	memset((void *)(reconstruct_ptr + line_size), 0, YYJSON_PADDING_SIZE);
	buffer_offset += part2_size;

	// We copied the object, so we are no longer reading the previous buffer
	if (--previous_buffer_handle->readers == 0) {
		current_reader->RemoveBuffer(current_buffer_handle->buffer_index - 1);
	}

	values[0] = ParseLine((char *)reconstruct_ptr, line_size, line_size, lines[0]);
}

void JSONScanLocalState::ReadUnstructured(idx_t &count) {
	// yyjson does not always return YYJSON_READ_ERROR_UNEXPECTED_END properly
	// if a different error code happens within the last 50 bytes
	// we assume it should be YYJSON_READ_ERROR_UNEXPECTED_END instead
	static constexpr idx_t END_BOUND = 50;

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
			lines_or_objects_in_buffer++;
		} else if (error.pos > max_obj_size) {
			current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, error,
			                                "Try increasing \"maximum_object_size\".");
		} else if (!is_last && (error.code == YYJSON_READ_ERROR_UNEXPECTED_END || remaining - error.pos < END_BOUND)) {
			// Copy remaining to reconstruct_buffer
			const auto reconstruct_ptr = reconstruct_buffer.get();
			memcpy(reconstruct_ptr, obj_copy_start, remaining);
			prev_buffer_remainder = remaining;
			buffer_offset = buffer_size;
			break;
		} else {
			current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, error);
		}
		values[count] = read_doc->root;
	}
}

void JSONScanLocalState::ReadNewlineDelimited(idx_t &count) {
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

		values[count] = ParseLine((char *)line_start, line_size, remaining, lines[count]);

		buffer_offset += line_size;
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}
}

yyjson_alc *JSONScanLocalState::GetAllocator() {
	return json_allocator.GetYYJSONAllocator();
}

void JSONScanLocalState::ThrowTransformError(idx_t object_index, const string &error_message) {
	D_ASSERT(current_reader);
	D_ASSERT(current_buffer_handle);
	D_ASSERT(object_index != DConstants::INVALID_INDEX);
	auto line_or_object_in_buffer = lines_or_objects_in_buffer - scan_count + object_index;
	current_reader->ThrowTransformError(current_buffer_handle->buffer_index, line_or_object_in_buffer, error_message);
}

} // namespace duckdb
