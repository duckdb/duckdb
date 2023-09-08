#include "json_scan.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

JSONScanData::JSONScanData() {
}

JSONScanData::JSONScanData(ClientContext &context, vector<string> files_p, string date_format_p,
                           string timestamp_format_p)
    : files(std::move(files_p)), date_format(std::move(date_format_p)),
      timestamp_format(std::move(timestamp_format_p)) {
	InitializeReaders(context);
	InitializeFormats();
}

void JSONScanData::Bind(ClientContext &context, TableFunctionBindInput &input) {
	auto &info = input.info->Cast<JSONScanInfo>();
	type = info.type;
	options.format = info.format;
	options.record_type = info.record_type;
	auto_detect = info.auto_detect;

	for (auto &kv : input.named_parameters) {
		if (MultiFileReader::ParseOption(kv.first, kv.second, options.file_options, context)) {
			continue;
		}
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "ignore_errors") {
			ignore_errors = BooleanValue::Get(kv.second);
		} else if (loption == "maximum_object_size") {
			maximum_object_size = MaxValue<idx_t>(UIntegerValue::Get(kv.second), maximum_object_size);
		} else if (loption == "format") {
			auto arg = StringUtil::Lower(StringValue::Get(kv.second));
			static const auto FORMAT_OPTIONS =
			    case_insensitive_map_t<JSONFormat> {{"auto", JSONFormat::AUTO_DETECT},
			                                        {"unstructured", JSONFormat::UNSTRUCTURED},
			                                        {"newline_delimited", JSONFormat::NEWLINE_DELIMITED},
			                                        {"nd", JSONFormat::NEWLINE_DELIMITED},
			                                        {"array", JSONFormat::ARRAY}};
			auto lookup = FORMAT_OPTIONS.find(arg);
			if (lookup == FORMAT_OPTIONS.end()) {
				vector<string> valid_options;
				for (auto &pair : FORMAT_OPTIONS) {
					valid_options.push_back(StringUtil::Format("'%s'", pair.first));
				}
				throw BinderException("format must be one of [%s], not '%s'", StringUtil::Join(valid_options, ", "),
				                      arg);
			}
			options.format = lookup->second;
		} else if (loption == "compression") {
			SetCompression(StringUtil::Lower(StringValue::Get(kv.second)));
		}
	}

	files = MultiFileReader::GetFileList(context, input.inputs[0], "JSON");
	options.file_options.AutoDetectHivePartitioning(files, context);

	InitializeReaders(context);
}

void JSONScanData::InitializeReaders(ClientContext &context) {
	union_readers.resize(files.empty() ? 0 : files.size() - 1);
	for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
		if (file_idx == 0) {
			initial_reader = make_uniq<BufferedJSONReader>(context, options, files[0]);
		} else {
			union_readers[file_idx - 1] = make_uniq<BufferedJSONReader>(context, options, files[file_idx]);
		}
	}
}

void JSONScanData::InitializeFormats() {
	InitializeFormats(auto_detect);
}

void JSONScanData::InitializeFormats(bool auto_detect_p) {
	// Initialize date_format_map if anything was specified
	if (!date_format.empty()) {
		date_format_map.AddFormat(LogicalTypeId::DATE, date_format);
	}
	if (!timestamp_format.empty()) {
		date_format_map.AddFormat(LogicalTypeId::TIMESTAMP, timestamp_format);
	}

	if (auto_detect_p) {
		static const type_id_map_t<vector<const char *>> FORMAT_TEMPLATES = {
		    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
		    {LogicalTypeId::TIMESTAMP,
		     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
		      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"}},
		};

		// Populate possible date/timestamp formats, assume this is consistent across columns
		for (auto &kv : FORMAT_TEMPLATES) {
			const auto &logical_type = kv.first;
			if (date_format_map.HasFormats(logical_type)) {
				continue; // Already populated
			}
			const auto &format_strings = kv.second;
			for (auto &format_string : format_strings) {
				date_format_map.AddFormat(logical_type, format_string);
			}
		}
	}
}

void JSONScanData::SetCompression(const string &compression) {
	options.compression = EnumUtil::FromString<FileCompressionType>(StringUtil::Upper(compression));
}

string JSONScanData::GetDateFormat() const {
	if (!date_format.empty()) {
		return date_format;
	} else if (date_format_map.HasFormats(LogicalTypeId::DATE)) {
		return date_format_map.GetFormat(LogicalTypeId::DATE).format_specifier;
	} else {
		return string();
	}
}

string JSONScanData::GetTimestampFormat() const {
	if (!timestamp_format.empty()) {
		return timestamp_format;
	} else if (date_format_map.HasFormats(LogicalTypeId::TIMESTAMP)) {
		return date_format_map.GetFormat(LogicalTypeId::TIMESTAMP).format_specifier;
	} else {
		return string();
	}
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, const JSONScanData &bind_data_p)
    : bind_data(bind_data_p), transform_options(bind_data.transform_options),
      allocator(BufferManager::GetBufferManager(context).GetBufferAllocator()),
      buffer_capacity(bind_data.maximum_object_size * 2), file_index(0), batch_index(0),
      system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()),
      enable_parallel_scans(bind_data.files.size() < system_threads) {
}

JSONScanLocalState::JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate)
    : scan_count(0), batch_index(DConstants::INVALID_INDEX), total_read_size(0), total_tuple_count(0),
      bind_data(gstate.bind_data), allocator(BufferAllocator::Get(context)), current_reader(nullptr),
      current_buffer_handle(nullptr), is_last(false), buffer_size(0), buffer_offset(0), prev_buffer_remainder(0) {

	// Buffer to reconstruct JSON values when they cross a buffer boundary
	reconstruct_buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
}

JSONGlobalTableFunctionState::JSONGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input)
    : state(context, input.bind_data->Cast<JSONScanData>()) {
}

unique_ptr<GlobalTableFunctionState> JSONGlobalTableFunctionState::Init(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<JSONScanData>();
	auto result = make_uniq<JSONGlobalTableFunctionState>(context, input);
	auto &gstate = result->state;

	// Perform projection pushdown
	for (idx_t col_idx = 0; col_idx < input.column_ids.size(); col_idx++) {
		const auto &col_id = input.column_ids[col_idx];

		// Skip any multi-file reader / row id stuff
		if (col_id == bind_data.reader_bind.filename_idx || IsRowIdColumnId(col_id)) {
			continue;
		}
		bool skip = false;
		for (const auto &hive_partitioning_index : bind_data.reader_bind.hive_partitioning_indexes) {
			if (col_id == hive_partitioning_index.index) {
				skip = true;
				break;
			}
		}
		if (skip) {
			continue;
		}

		gstate.column_indices.push_back(col_idx);
		gstate.names.push_back(bind_data.names[col_id]);
	}

	if (gstate.names.size() < bind_data.names.size() || bind_data.options.file_options.union_by_name) {
		// If we are auto-detecting, but don't need all columns present in the file,
		// then we don't need to throw an error if we encounter an unseen column
		gstate.transform_options.error_unknown_key = false;
	}

	// Place readers where they belong
	if (bind_data.initial_reader) {
		bind_data.initial_reader->Reset();
		gstate.json_readers.emplace_back(bind_data.initial_reader.get());
	}
	for (const auto &reader : bind_data.union_readers) {
		reader->Reset();
		gstate.json_readers.emplace_back(reader.get());
	}

	vector<LogicalType> dummy_types(input.column_ids.size(), LogicalType::ANY);
	for (auto &reader : gstate.json_readers) {
		MultiFileReader::FinalizeBind(reader->GetOptions().file_options, gstate.bind_data.reader_bind,
		                              reader->GetFileName(), gstate.names, dummy_types, bind_data.names,
		                              input.column_ids, reader->reader_data, context);
	}

	return std::move(result);
}

idx_t JSONGlobalTableFunctionState::MaxThreads() const {
	auto &bind_data = state.bind_data;
	if (bind_data.options.format == JSONFormat::NEWLINE_DELIMITED) {
		return state.system_threads;
	}

	if (!state.json_readers.empty() && state.json_readers[0]->HasFileHandle()) {
		auto &reader = *state.json_readers[0];
		if (reader.GetFormat() == JSONFormat::NEWLINE_DELIMITED) { // Auto-detected NDJSON
			return state.system_threads;
		}
	}

	// One reader per file
	return bind_data.files.size();
}

JSONLocalTableFunctionState::JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate)
    : state(context, gstate) {
}

unique_ptr<LocalTableFunctionState> JSONLocalTableFunctionState::Init(ExecutionContext &context,
                                                                      TableFunctionInitInput &,
                                                                      GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<JSONGlobalTableFunctionState>();
	auto result = make_uniq<JSONLocalTableFunctionState>(context.client, gstate.state);

	// Copy the transform options / date format map because we need to do thread-local stuff
	result->state.date_format_map = gstate.state.bind_data.date_format_map;
	result->state.transform_options = gstate.state.transform_options;
	result->state.transform_options.date_format_map = &result->state.date_format_map;

	return std::move(result);
}

idx_t JSONLocalTableFunctionState::GetBatchIndex() const {
	return state.batch_index;
}

static inline void SkipWhitespace(const char *buffer_ptr, idx_t &buffer_offset, const idx_t &buffer_size) {
	for (; buffer_offset != buffer_size; buffer_offset++) {
		if (!StringUtil::CharacterIsSpace(buffer_ptr[buffer_offset])) {
			break;
		}
	}
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	allocator.Reset();
	scan_count = 0;

	// We have to wrap this in a loop otherwise we stop scanning too early when there's an empty JSON file
	while (scan_count == 0) {
		if (buffer_offset == buffer_size) {
			if (!ReadNextBuffer(gstate)) {
				break;
			}
			D_ASSERT(buffer_size != 0);
			if (current_buffer_handle->buffer_index != 0 &&
			    current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
				ReconstructFirstObject();
				scan_count++;
			}
		}

		ParseNextChunk();
	}

	return scan_count;
}

static inline const char *NextNewline(char *ptr, idx_t size) {
	return char_ptr_cast(memchr(ptr, '\n', size));
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

static inline void TrimWhitespace(JSONString &line) {
	while (line.size != 0 && StringUtil::CharacterIsSpace(line[0])) {
		line.pointer++;
		line.size--;
	}
	while (line.size != 0 && StringUtil::CharacterIsSpace(line[line.size - 1])) {
		line.size--;
	}
}

void JSONScanLocalState::ParseJSON(char *const json_start, const idx_t json_size, const idx_t remaining) {
	yyjson_doc *doc;
	yyjson_read_err err;
	if (bind_data.type == JSONScanType::READ_JSON_OBJECTS) { // If we return strings, we cannot parse INSITU
		doc = JSONCommon::ReadDocumentUnsafe(json_start, json_size, JSONCommon::READ_STOP_FLAG, allocator.GetYYAlc(),
		                                     &err);
	} else {
		doc = JSONCommon::ReadDocumentUnsafe(json_start, remaining, JSONCommon::READ_INSITU_FLAG, allocator.GetYYAlc(),
		                                     &err);
	}
	if (!bind_data.ignore_errors && err.code != YYJSON_READ_SUCCESS) {
		current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, err);
	}

	// We parse with YYJSON_STOP_WHEN_DONE, so we need to check this by hand
	const auto read_size = yyjson_doc_get_read_size(doc);
	if (read_size > json_size) {
		// Can't go past the boundary, even with ignore_errors
		err.code = YYJSON_READ_ERROR_UNEXPECTED_END;
		err.msg = "unexpected end of data";
		err.pos = json_size;
		current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, err,
		                                "Try auto-detecting the JSON format");
	} else if (!bind_data.ignore_errors && read_size < json_size) {
		idx_t off = read_size;
		idx_t rem = json_size;
		SkipWhitespace(json_start, off, rem);
		if (off != rem) { // Between end of document and boundary should be whitespace only
			err.code = YYJSON_READ_ERROR_UNEXPECTED_CONTENT;
			err.msg = "unexpected content after document";
			err.pos = read_size;
			current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, err,
			                                "Try auto-detecting the JSON format");
		}
	}

	lines_or_objects_in_buffer++;
	if (!doc) {
		values[scan_count] = nullptr;
		return;
	}

	// Set the JSONLine and trim
	units[scan_count] = JSONString(json_start, json_size);
	TrimWhitespace(units[scan_count]);
	values[scan_count] = doc->root;
}

void JSONScanLocalState::ThrowObjectSizeError(const idx_t object_size) {
	throw InvalidInputException(
	    "\"maximum_object_size\" of %llu bytes exceeded while reading file \"%s\" (>%llu bytes)."
	    "\n Try increasing \"maximum_object_size\".",
	    bind_data.maximum_object_size, current_reader->GetFileName(), object_size);
}

void JSONScanLocalState::ThrowInvalidAtEndError() {
	throw InvalidInputException("Invalid JSON detected at the end of file \"%s\".", current_reader->GetFileName());
}

void JSONScanLocalState::TryIncrementFileIndex(JSONScanGlobalState &gstate) const {
	lock_guard<mutex> guard(gstate.lock);
	if (gstate.file_index < gstate.json_readers.size() &&
	    current_reader.get() == gstate.json_readers[gstate.file_index].get()) {
		gstate.file_index++;
	}
}

bool JSONScanLocalState::IsParallel(JSONScanGlobalState &gstate) const {
	if (bind_data.files.size() >= gstate.system_threads) {
		return false; // More files than threads, just parallelize over the files
	}

	// NDJSON can be read in parallel
	return current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED;
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

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate) {
	// First we make sure we have a buffer to read into
	AllocatedData buffer;

	// Try to re-use a buffer that was used before
	if (current_reader) {
		current_reader->SetBufferLineOrObjectCount(current_buffer_handle->buffer_index, lines_or_objects_in_buffer);
		if (current_buffer_handle && --current_buffer_handle->readers == 0) {
			buffer = current_reader->RemoveBuffer(current_buffer_handle->buffer_index);
		}
	}

	// If we cannot re-use a buffer we create a new one
	if (!buffer.IsSet()) {
		buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
	}

	buffer_ptr = char_ptr_cast(buffer.get());

	// Copy last bit of previous buffer
	if (current_reader && current_reader->GetFormat() != JSONFormat::NEWLINE_DELIMITED && !is_last) {
		memcpy(buffer_ptr, reconstruct_buffer.get(), prev_buffer_remainder);
	}

	optional_idx buffer_index;
	while (true) {
		// Now we finish the current reader
		if (current_reader) {
			// If we performed the final read of this reader in the previous iteration, close it now
			if (is_last) {
				if (gstate.bind_data.type != JSONScanType::SAMPLE) {
					TryIncrementFileIndex(gstate);
					current_reader->CloseJSONFile();
				}
				current_reader = nullptr;
				continue;
			}

			// Try to read
			ReadNextBufferInternal(gstate, buffer_index);

			// If this is the last read, end the parallel scan now so threads can move on
			if (is_last && IsParallel(gstate)) {
				TryIncrementFileIndex(gstate);
			}

			if (buffer_size == 0) {
				// We didn't read anything, re-enter the loop
				continue;
			} else {
				// We read something!
				break;
			}
		}

		// If we got here, we don't have a reader (anymore). Try to get one
		is_last = false;
		{
			lock_guard<mutex> guard(gstate.lock);
			if (gstate.file_index == gstate.json_readers.size()) {
				return false; // No more files left
			}

			// Assign the next reader to this thread
			current_reader = gstate.json_readers[gstate.file_index].get();

			// Open the file if it is not yet open
			if (!current_reader->IsOpen()) {
				current_reader->OpenJSONFile();
			}
			batch_index = gstate.batch_index++;

			// Auto-detect format / record type
			if (gstate.enable_parallel_scans) {
				// Auto-detect within the lock, so threads may join a parallel NDJSON scan
				if (current_reader->GetFormat() == JSONFormat::AUTO_DETECT) {
					ReadAndAutoDetect(gstate, buffer_index);
				}
			} else {
				gstate.file_index++; // Increment the file index before dropping lock so other threads move on
			}
		}

		// If we didn't auto-detect within the lock, do it now
		if (current_reader->GetFormat() == JSONFormat::AUTO_DETECT) {
			ReadAndAutoDetect(gstate, buffer_index);
		}

		// If we haven't already, increment the file index if non-parallel scan
		if (gstate.enable_parallel_scans && !IsParallel(gstate)) {
			TryIncrementFileIndex(gstate);
		}

		if (!buffer_index.IsValid() || buffer_size == 0) {
			// If we didn't get a buffer index (because not auto-detecting), or the file was empty, just re-enter loop
			continue;
		}

		break;
	}
	D_ASSERT(buffer_size != 0); // We should have read something if we got here
	D_ASSERT(buffer_index.IsValid());

	idx_t readers = 1;
	if (current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
		readers = is_last ? 1 : 2;
	}

	// Create an entry and insert it into the map
	auto json_buffer_handle =
	    make_uniq<JSONBufferHandle>(buffer_index.GetIndex(), readers, std::move(buffer), buffer_size);
	current_buffer_handle = json_buffer_handle.get();
	current_reader->InsertBuffer(buffer_index.GetIndex(), std::move(json_buffer_handle));

	prev_buffer_remainder = 0;
	lines_or_objects_in_buffer = 0;

	// YYJSON needs this
	memset(buffer_ptr + buffer_size, 0, YYJSON_PADDING_SIZE);

	return true;
}

void JSONScanLocalState::ReadAndAutoDetect(JSONScanGlobalState &gstate, optional_idx &buffer_index) {
	// We have to detect the JSON format - hold the gstate lock while we do this
	ReadNextBufferInternal(gstate, buffer_index);
	if (buffer_size == 0) {
		return;
	}

	auto format_and_record_type = DetectFormatAndRecordType(buffer_ptr, buffer_size, allocator.GetYYAlc());
	current_reader->SetFormat(format_and_record_type.first);
	if (current_reader->GetRecordType() == JSONRecordType::AUTO_DETECT) {
		current_reader->SetRecordType(format_and_record_type.second);
	}
	if (current_reader->GetFormat() == JSONFormat::ARRAY) {
		SkipOverArrayStart();
	}

	if (bind_data.options.record_type == JSONRecordType::RECORDS &&
	    current_reader->GetRecordType() != JSONRecordType::RECORDS) {
		throw InvalidInputException("Expected file \"%s\" to contain records, detected non-record JSON instead.",
		                            current_reader->GetFileName());
	}
}

void JSONScanLocalState::ReadNextBufferInternal(JSONScanGlobalState &gstate, optional_idx &buffer_index) {
	if (current_reader->GetFileHandle().CanSeek()) {
		ReadNextBufferSeek(gstate, buffer_index);
	} else {
		ReadNextBufferNoSeek(gstate, buffer_index);
	}

	buffer_offset = 0;
	if (buffer_index.GetIndex() == 0 && current_reader->GetFormat() == JSONFormat::ARRAY) {
		SkipOverArrayStart();
	}
}

void JSONScanLocalState::ReadNextBufferSeek(JSONScanGlobalState &gstate, optional_idx &buffer_index) {
	auto &file_handle = current_reader->GetFileHandle();

	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_position;
	idx_t read_size;

	{
		lock_guard<mutex> reader_guard(current_reader->lock);
		buffer_index = current_reader->GetBufferIndex();

		read_size = file_handle.GetPositionAndSize(read_position, request_size);
		is_last = read_size < request_size;

		if (!gstate.bind_data.ignore_errors && read_size == 0 && prev_buffer_remainder != 0) {
			ThrowInvalidAtEndError();
		}

		if (read_size != 0 && current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
			batch_index = gstate.batch_index++;
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
	if (buffer_size == 0) {
		current_reader->SetBufferLineOrObjectCount(buffer_index.GetIndex(), 0);
		return;
	}

	// Now read the file lock-free!
	file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, read_size, read_position,
	                           gstate.bind_data.type == JSONScanType::SAMPLE);
}

void JSONScanLocalState::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, optional_idx &buffer_index) {
	idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder - YYJSON_PADDING_SIZE;
	idx_t read_size;
	{
		lock_guard<mutex> reader_guard(current_reader->lock);
		buffer_index = current_reader->GetBufferIndex();

		if (current_reader->HasFileHandle() && current_reader->IsOpen()) {
			read_size = current_reader->GetFileHandle().Read(buffer_ptr + prev_buffer_remainder, request_size,
			                                                 gstate.bind_data.type == JSONScanType::SAMPLE);
			is_last = read_size < request_size;
		} else {
			read_size = 0;
			is_last = true;
		}

		if (!gstate.bind_data.ignore_errors && read_size == 0 && prev_buffer_remainder != 0) {
			ThrowInvalidAtEndError();
		}

		if (read_size != 0 && current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED) {
			batch_index = gstate.batch_index++;
		}
	}
	buffer_size = prev_buffer_remainder + read_size;
	if (buffer_size == 0) {
		current_reader->SetBufferLineOrObjectCount(buffer_index.GetIndex(), 0);
		return;
	}
}

void JSONScanLocalState::SkipOverArrayStart() {
	// First read of this buffer, check if it's actually an array and skip over the bytes
	SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	if (buffer_offset == buffer_size) {
		return; // Empty file
	}
	if (buffer_ptr[buffer_offset] != '[') {
		throw InvalidInputException(
		    "Expected top-level JSON array with format='array', but first character is '%c' in file \"%s\"."
		    "\n Try setting format='auto' or format='newline_delimited'.",
		    buffer_ptr[buffer_offset], current_reader->GetFileName());
	}
	SkipWhitespace(buffer_ptr, ++buffer_offset, buffer_size);
	if (buffer_offset >= buffer_size) {
		throw InvalidInputException("Missing closing brace ']' in JSON array with format='array' in file \"%s\"",
		                            current_reader->GetFileName());
	}
	if (buffer_ptr[buffer_offset] == ']') {
		// Empty array
		SkipWhitespace(buffer_ptr, ++buffer_offset, buffer_size);
		if (buffer_offset != buffer_size) {
			throw InvalidInputException(
			    "Empty array with trailing data when parsing JSON array with format='array' in file \"%s\"",
			    current_reader->GetFileName());
		}
		return;
	}
}

void JSONScanLocalState::ReconstructFirstObject() {
	D_ASSERT(current_buffer_handle->buffer_index != 0);
	D_ASSERT(current_reader->GetFormat() == JSONFormat::NEWLINE_DELIMITED);

	// Spinlock until the previous batch index has also read its buffer
	optional_ptr<JSONBufferHandle> previous_buffer_handle;
	while (!previous_buffer_handle) {
		previous_buffer_handle = current_reader->GetBuffer(current_buffer_handle->buffer_index - 1);
	}

	// First we find the newline in the previous block
	auto prev_buffer_ptr = char_ptr_cast(previous_buffer_handle->buffer.get()) + previous_buffer_handle->buffer_size;
	auto part1_ptr = PreviousNewline(prev_buffer_ptr);
	auto part1_size = prev_buffer_ptr - part1_ptr;

	// Now copy the data to our reconstruct buffer
	const auto reconstruct_ptr = reconstruct_buffer.get();
	memcpy(reconstruct_ptr, part1_ptr, part1_size);
	// Now find the newline in the current block
	auto line_end = NextNewline(buffer_ptr, buffer_size);
	if (line_end == nullptr) {
		ThrowObjectSizeError(buffer_size - buffer_offset);
	} else {
		line_end++;
	}
	idx_t part2_size = line_end - buffer_ptr;

	idx_t line_size = part1_size + part2_size;
	if (line_size > bind_data.maximum_object_size) {
		ThrowObjectSizeError(line_size);
	}

	// And copy the remainder of the line to the reconstruct buffer
	memcpy(reconstruct_ptr + part1_size, buffer_ptr, part2_size);
	memset(reconstruct_ptr + line_size, 0, YYJSON_PADDING_SIZE);
	buffer_offset += part2_size;

	// We copied the object, so we are no longer reading the previous buffer
	if (--previous_buffer_handle->readers == 0) {
		current_reader->RemoveBuffer(current_buffer_handle->buffer_index - 1);
	}

	ParseJSON(char_ptr_cast(reconstruct_ptr), line_size, line_size);
}

void JSONScanLocalState::ParseNextChunk() {
	auto buffer_offset_before = buffer_offset;

	const auto format = current_reader->GetFormat();
	for (; scan_count < STANDARD_VECTOR_SIZE; scan_count++) {
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
		auto json_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;
		if (remaining == 0) {
			break;
		}
		const char *json_end = format == JSONFormat::NEWLINE_DELIMITED ? NextNewline(json_start, remaining)
		                                                               : NextJSON(json_start, remaining);
		if (json_end == nullptr) {
			// We reached the end of the buffer
			if (!is_last) {
				// Last bit of data belongs to the next batch
				if (format != JSONFormat::NEWLINE_DELIMITED) {
					if (scan_count == 0) {
						ThrowObjectSizeError(remaining);
					}
					memcpy(reconstruct_buffer.get(), json_start, remaining);
					prev_buffer_remainder = remaining;
				}
				buffer_offset = buffer_size;
				break;
			}
			json_end = json_start + remaining;
		}

		idx_t json_size = json_end - json_start;
		ParseJSON(json_start, json_size, remaining);
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
				current_reader->ThrowParseError(current_buffer_handle->buffer_index, lines_or_objects_in_buffer, err);
			}
		}
		SkipWhitespace(buffer_ptr, buffer_offset, buffer_size);
	}

	total_read_size += buffer_offset - buffer_offset_before;
	total_tuple_count += scan_count;
}

yyjson_alc *JSONScanLocalState::GetAllocator() {
	return allocator.GetYYAlc();
}

const MultiFileReaderData &JSONScanLocalState::GetReaderData() const {
	return current_reader->reader_data;
}

void JSONScanLocalState::ThrowTransformError(idx_t object_index, const string &error_message) {
	D_ASSERT(current_reader);
	D_ASSERT(current_buffer_handle);
	D_ASSERT(object_index != DConstants::INVALID_INDEX);
	auto line_or_object_in_buffer = lines_or_objects_in_buffer - scan_count + object_index;
	current_reader->ThrowTransformError(current_buffer_handle->buffer_index, line_or_object_in_buffer, error_message);
}

double JSONScan::ScanProgress(ClientContext &, const FunctionData *, const GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<JSONGlobalTableFunctionState>().state;
	double progress = 0;
	for (auto &reader : gstate.json_readers) {
		progress += reader->GetProgress();
	}
	return progress / double(gstate.json_readers.size());
}

idx_t JSONScan::GetBatchIndex(ClientContext &, const FunctionData *, LocalTableFunctionState *local_state,
                              GlobalTableFunctionState *) {
	auto &lstate = local_state->Cast<JSONLocalTableFunctionState>();
	return lstate.GetBatchIndex();
}

unique_ptr<NodeStatistics> JSONScan::Cardinality(ClientContext &, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<JSONScanData>();
	idx_t per_file_cardinality;
	if (data.initial_reader && data.initial_reader->HasFileHandle()) {
		per_file_cardinality = data.initial_reader->GetFileHandle().FileSize() / data.avg_tuple_size;
	} else {
		per_file_cardinality = 42; // The cardinality of an unknown JSON file is the almighty number 42
	}
	return make_uniq<NodeStatistics>(per_file_cardinality * data.files.size());
}

void JSONScan::ComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                     vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data_p->Cast<JSONScanData>();
	auto reset_reader =
	    MultiFileReader::ComplexFilterPushdown(context, data.files, data.options.file_options, get, filters);
	if (reset_reader) {
		MultiFileReader::PruneReaders(data);
	}
}

void JSONScan::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p, const TableFunction &) {
	auto &bind_data = bind_data_p->Cast<JSONScanData>();
	serializer.WriteProperty(100, "scan_data", &bind_data);
}

unique_ptr<FunctionData> JSONScan::Deserialize(Deserializer &deserializer, TableFunction &) {
	unique_ptr<JSONScanData> result;
	deserializer.ReadProperty(100, "scan_data", result);
	result->InitializeReaders(deserializer.Get<ClientContext &>());
	result->InitializeFormats();
	result->transform_options.date_format_map = &result->date_format_map;
	return std::move(result);
}

void JSONScan::TableFunctionDefaults(TableFunction &table_function) {
	MultiFileReader::AddParameters(table_function);

	table_function.named_parameters["maximum_object_size"] = LogicalType::UINTEGER;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["format"] = LogicalType::VARCHAR;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;

	table_function.table_scan_progress = ScanProgress;
	table_function.get_batch_index = GetBatchIndex;
	table_function.cardinality = Cardinality;

	table_function.serialize = Serialize;
	table_function.deserialize = Deserialize;

	table_function.projection_pushdown = true;
	table_function.filter_pushdown = false;
	table_function.filter_prune = false;
	table_function.pushdown_complex_filter = ComplexFilterPushdown;
}

} // namespace duckdb
