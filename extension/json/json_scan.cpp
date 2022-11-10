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

	return make_unique<JSONScanData>(options);
}

JSONBufferHandle::JSONBufferHandle(idx_t readers_p, AllocatedData &&buffer_p)
    : readers(readers_p), buffer(move(buffer_p)) {
}

JSONScanGlobalState::JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data)
    : json_reader(make_unique<BufferedJSONReader>(context, bind_data.options)), batch_index(0),
      allocator(BufferManager::GetBufferManager(context).GetBufferAllocator()) {
	json_reader->OpenJSONFile();
}

unique_ptr<GlobalTableFunctionState> JSONScanGlobalState::Init(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (JSONScanData &)*input.bind_data;
	return make_unique<JSONScanGlobalState>(context, bind_data);
}

JSONScanLocalState::JSONScanLocalState(JSONScanGlobalState &gstate) : current_buffer_handle(nullptr), ptr(nullptr) {
	// TODO: maybe options.maximum_object_size
	reconstruct_buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	auto &gstate = (JSONScanGlobalState &)*global_state;
	return make_unique<JSONScanLocalState>(gstate);
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	// TODO split so this does not become a ridiculously big function
	if (ptr == nullptr) {
		auto &file_handle = gstate.json_reader->GetFileHandle();

		idx_t read_position;
		idx_t readers;
		{
			lock_guard<mutex> guard(gstate.lock);
			buffer_remaining = file_handle.GetPositionAndSize(read_position, gstate.buffer_capacity);
			if (buffer_remaining == 0) {
				// We reached the end of the file
				return 0;
			}

			// Get batch index and create an entry in the buffer map
			batch_index = gstate.batch_index++;
			readers = file_handle.Remaining() == 0 ? 1 : 2;
		}

		auto &buffer_map = gstate.buffer_map;
		AllocatedData buffer;
		if (current_buffer_handle && --current_buffer_handle->readers == 0) {
			// Take ownership of the previous buffer and remove entry from map
			lock_guard<mutex> guard(gstate.lock);
			buffer = move(current_buffer_handle->buffer);
			buffer_map.erase(batch_index);
		} else {
			// Allocate a new buffer
			buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
		}
		ptr = (const char *)buffer.get();

		// Now read the file lock-free!
		file_handle.Read(ptr, buffer_remaining, read_position);

		// Insert it into the map
		{
			lock_guard<mutex> guard(gstate.lock);
			current_buffer_handle = &buffer_map
			                             .emplace(std::piecewise_construct, std::make_tuple(batch_index),
			                                      std::make_tuple(readers, move(buffer)))
			                             .first->second;
		}

		if (read_position != 0) {
			D_ASSERT(batch_index != 0);
			// Spinlock until the previous batch index has also read its buffer
			while (true) {
				lock_guard<mutex> guard(gstate.lock);
				auto it = buffer_map.find(batch_index - 1);
				if (it == buffer_map.end()) {
					continue;
				}
				// We found it in the map!
				previous_buffer_handle = &it->second;
				break;
			}

			// TODO: reconstruct first JSON obj here!
			auto &prev_buffer = previous_buffer_handle->buffer;
			auto prev_ptr = (const char *)prev_buffer.get() + prev_buffer.GetSize() - 1;
			idx_t size = 0;
			while (*prev_ptr != '\n') {
				prev_ptr--;
				size++;
			}
			auto reconstruct_ptr = reconstruct_buffer.get();
			memcpy(reconstruct_ptr, prev_ptr, size);


			auto line_end = (const char *)memchr(ptr, '\n', buffer_remaining);
			memcpy(prev_ptr + size, ptr, line_end - ptr);
			ptr

			// TODO: maybe throw error here saying json object is too big?
		}
	}

	// There is data left in the buffer, scan line-by-line
	idx_t count;
	auto line_start = ptr;
	for (count = 0; count < STANDARD_VECTOR_SIZE; count++) {
		auto line_end = (const char *)memchr(line_start, '\n', buffer_remaining);
		if (line_end == nullptr) {
			// We reached the end of the buffer
			line_start = nullptr;
			buffer_remaining = 0;
			break;
		}
		// Don't include the '\n' in the line
		lines[count].pointer = (const char *)line_start;
		lines[count].size = line_end - line_start;
		// Skip over '\n'
		line_start = ++line_end;
		buffer_remaining -= lines[count].size + 1;
	}
	ptr = line_start;
	// TODO maybe throw error saying json object is too big if we don't find anything?

	return count;
}

} // namespace duckdb
