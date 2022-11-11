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
	return json_reader->MaxThreads(buffer_capacity);
}

JSONScanLocalState::JSONScanLocalState(JSONScanGlobalState &gstate)
    : current_buffer_handle(nullptr), buffer_size(0), buffer_offset(0) {
	// TODO: maybe options.maximum_object_size
	reconstruct_buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
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

		if (!first_read) {
			ReconstructFirstObject(gstate);
			count++;
		}
	}

	// There is data left in the buffer, scan line-by-line
	for (; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_start = buffer_ptr + buffer_offset;
		idx_t remaining = buffer_size - buffer_offset;

		// Search for newline
		auto line_end = (const char *)memchr(line_start, '\n', remaining);
		if (line_end == nullptr) {
			// We reached the end of the buffer
			if (!is_last) {
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
		lines[count].pointer = (const char *)line_start;
		lines[count].size = line_size;

		buffer_offset += line_size;
	}

	if (count == 0) {
		// TODO if last scan perfectly read everything, but left a tiny buffer left then this will trigger incorrectly
		throw InvalidInputException("TODO: json obj too big bla");
	}

	return count;
}

bool JSONScanLocalState::ReadNextBuffer(JSONScanGlobalState &gstate, bool &first_read) {
	auto &file_handle = gstate.json_reader->GetFileHandle();

	idx_t readers;
	idx_t read_position;
	idx_t next_batch_index;
	{
		lock_guard<mutex> guard(gstate.lock);
		buffer_size = file_handle.GetPositionAndSize(read_position, gstate.buffer_capacity);
		buffer_offset = 0;
		if (buffer_size == 0) {
			// We reached the end of the file
			return false;
		}

		// Get batch index and create an entry in the buffer map
		next_batch_index = gstate.batch_index++;
		is_last = file_handle.Remaining() == 0;
	}
	readers = is_last ? 1 : 2;
	first_read = read_position == 0;

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

	// Now read the file lock-free!
	file_handle.Read(buffer_ptr, buffer_size, read_position);

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

void JSONScanLocalState::ReconstructFirstObject(JSONScanGlobalState &gstate) {
	D_ASSERT(batch_index != 0);
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

	// Reconstruct first JSON object that was split
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
		buffer_map.erase(batch_index);
	}
	previous_buffer_handle = nullptr;
	// Set the pointer/size
	lines[0].pointer = (const char *)reconstruct_ptr;
	lines[0].size = part1_size + part2_size;
}

idx_t JSONScanLocalState::GetBatchIndex() const {
	return batch_index;
}

} // namespace duckdb
