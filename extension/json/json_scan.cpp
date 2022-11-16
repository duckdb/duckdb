#include "json_scan.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

JSONScanData::JSONScanData(BufferedJSONReaderOptions options) : options(move(options)) {
}

unique_ptr<FunctionData> JSONScanData::Bind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning JSON files is disabled through configuration");
	}

	BufferedJSONReaderOptions options;
	options.file_path = input.inputs[0].GetValue<string>();
	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "ignore_errors") {
			options.ignore_errors = BooleanValue::Get(kv.second);
		} else if (loption == "maximum_object_size") {
			options.maximum_object_size = UBigIntValue::Get(kv.second);
		}
	}

	// TODO change this for the actual json_scan
	return_types.push_back(JSONCommon::JSONType());
	names.emplace_back("json");

	auto &info = (JSONScanInfo &)*input.info;
	if (info.forced_format == JSONFormat::AUTO_DETECT) {
		throw NotImplementedException("Auto-detection of JSON format");
	}
	options.format = info.forced_format;
	options.return_json_strings = info.return_json_strings;

	return make_unique<JSONScanData>(options);
}

JSONBufferHandle::JSONBufferHandle(idx_t readers_p, AllocatedData &&buffer_p)
    : readers(readers_p), buffer(move(buffer_p)) {
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data)
    : buffer_capacity(INITIAL_BUFFER_CAPACITY),
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
	// TODO: maybe options.maximum_object_size
	reconstruct_buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
	objects.reserve(STANDARD_VECTOR_SIZE);
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	auto &gstate = (JSONScanGlobalState &)*global_state;
	return make_unique<JSONScanLocalState>(gstate);
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
		ReadUnstructured(count, gstate.json_reader->options.return_json_strings);
		break;
	case JSONFormat::NEWLINE_DELIMITED:
		ReadNewlineDelimited(count);
		break;
	default:
		throw InternalException("Unknown JSON format");
	}

	if (count == 0) {
		// TODO if last scan perfectly read everything, but left a tiny buffer left then this will trigger incorrectly
		throw InvalidInputException("TODO: json obj too big bla");
	}

	// Skip whitespace at start and end
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

	idx_t readers;
	idx_t next_batch_index;
	auto &file_handle = gstate.json_reader->GetFileHandle();
	if (file_handle.CanSeek()) {
		ReadNextBufferSeek(gstate, first_read, next_batch_index, readers);
	} else {
		ReadNextBufferNoSeek(gstate, first_read, next_batch_index, readers);
	}

	if (buffer_size == 0) {
		return false;
	}

	// Create an entry and insert it into the map
	batch_index = next_batch_index;
	auto json_buffer_handle = make_unique<JSONBufferHandle>(readers, move(buffer));
	current_buffer_handle = json_buffer_handle.get();
	{
		lock_guard<mutex> guard(gstate.lock);
		buffer_map.insert(make_pair(batch_index, move(json_buffer_handle)));
	}

	return true;
}

void JSONScanLocalState::ReadNextBufferSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &next_batch_index,
                                            idx_t &readers) {
	auto &file_handle = gstate.json_reader->GetFileHandle();
	auto &format = gstate.json_reader->options.format;
	D_ASSERT(format != JSONFormat::AUTO_DETECT); // should be detected by now

	idx_t request_size = gstate.buffer_capacity;
	if (format == JSONFormat::UNSTRUCTURED) {
		request_size -= prev_buffer_remainder;
		request_size -= YYJSON_PADDING_SIZE;
	}

	idx_t read_position;
	{
		lock_guard<mutex> guard(gstate.lock);
		buffer_size = file_handle.GetPositionAndSize(read_position, request_size);
		buffer_offset = 0;
		if (buffer_size == 0) {
			// We reached the end of the file
			return;
		}

		// Get batch index and create an entry in the buffer map
		next_batch_index = gstate.batch_index++;
		is_last = file_handle.Remaining() == 0;
	}

	if (format == JSONFormat::UNSTRUCTURED) {
		readers = 1;
	} else {
		readers = is_last ? 1 : 2;
	}
	first_read = read_position == 0;

	// Now read the file lock-free!
	if (format == JSONFormat::UNSTRUCTURED) {
		file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, buffer_size, read_position);
		buffer_size += prev_buffer_remainder;
		memset((void *)(buffer_ptr + buffer_size), 0, 4); // add padding for yyjson insitu
	} else {
		file_handle.ReadAtPosition(buffer_ptr, buffer_size, read_position);
	}
}

void JSONScanLocalState::ReadNextBufferNoSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &next_batch_index,
                                              idx_t &readers) {
	auto &file_handle = gstate.json_reader->GetFileHandle();
	auto &format = gstate.json_reader->options.format;
	lock_guard<mutex> guard(gstate.lock);
	idx_t request_size = gstate.buffer_capacity;
	if (format == JSONFormat::UNSTRUCTURED) {
		request_size -= prev_buffer_remainder;
		request_size -= YYJSON_PADDING_SIZE;
	}

	next_batch_index = gstate.batch_index++;
	first_read = file_handle.Remaining() == file_handle.FileSize();

	if (format == JSONFormat::UNSTRUCTURED) {
		buffer_size = file_handle.Read(buffer_ptr + prev_buffer_remainder, request_size);
		buffer_size += prev_buffer_remainder;
		memset((void *)(buffer_ptr + buffer_size), 0, 4); // add padding for yyjson insitu
	} else {
		buffer_size = file_handle.Read(buffer_ptr, request_size);
	}

	buffer_offset = 0;
	is_last = buffer_size < request_size;

	if (format == JSONFormat::UNSTRUCTURED) {
		readers = 1;
	} else {
		readers = is_last ? 1 : 2;
	}
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
	const auto prev_ptr = (const char *)prev_buffer.get() + prev_buffer.GetSize();
	idx_t part1_size = 1;
	while (*(prev_ptr - part1_size) != '\n') {
		part1_size++;
	}
	part1_size--;

	// Now copy the data to our reconstruct buffer
	const auto reconstruct_ptr = reconstruct_buffer.get();
	memcpy(reconstruct_ptr, prev_ptr - part1_size, part1_size);
	// Now find the newline in the current block
	auto line_end = (const char *)memchr(buffer_ptr, '\n', buffer_size);
	if (line_end == nullptr) {
		throw InvalidInputException("TODO: json obj too big bla");
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
	std::replace((char *)line_start, (char *)line_start + remaining, '\0', '"');
}

void JSONScanLocalState::ReadUnstructured(idx_t &count, bool return_strings) {
	objects.clear();
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;

		// Read next JSON doc
		objects.emplace_back(JSONCommon::ReadDocumentFromFileUnsafe((char *)line_start, remaining));
		auto &read_doc = objects.back();

		if (read_doc.IsNull()) {
			RestoreParsedString(line_start, remaining); // Always restore because we will re-parse
			objects.pop_back();
			if (!is_last) {
				// Copy remaining to reconstruct_buffer
				const auto reconstruct_ptr = reconstruct_buffer.get();
				memcpy(reconstruct_ptr, line_start, remaining);
			}

			prev_buffer_remainder = remaining;
			buffer_offset = buffer_size;
			break;
		}
		idx_t line_size = read_doc.ReadSize();
		RestoreParsedString(line_start, line_size);

		lines[count].pointer = line_start;
		lines[count].size = line_size;
		buffer_offset += line_size;
	}

	D_ASSERT(count == objects.size());
}

void JSONScanLocalState::ReadNewlineDelimited(idx_t &count) {
	// There is data left in the buffer, scan line-by-line
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;

		// Search for newline
		auto line_end = (const char *)memchr(line_start, '\n', remaining);
		if (line_end == nullptr) {
			// We reached the end of the buffer
			if (!is_last || remaining == 0) {
				// Last bit of data belongs to the next batch
				buffer_offset = buffer_size;
				break;
			}
			line_end = line_start + remaining;
		} else {
			// Include the newline
			line_end++;
		}

		idx_t line_size = line_end - line_start;
		if (line_size == 0) {
			throw InvalidInputException("TODO: oops");
		}

		// Create an entry
		lines[count].pointer = line_start;
		lines[count].size = line_size;
		buffer_offset += line_size;
	}
}

idx_t JSONScanLocalState::GetBatchIndex() const {
	return batch_index;
}

} // namespace duckdb
