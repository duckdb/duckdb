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

JSONBufferHandle::JSONBufferHandle(idx_t readers, AllocatedData &&buffer) : readers(readers), buffer(move(buffer)) {
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

JSONScanLocalState::JSONScanLocalState() : current_buffer_handle(nullptr), read_position(0), read_size(0) {
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	return make_unique<JSONScanLocalState>();
}

idx_t JSONScanLocalState::ReadNext(JSONScanGlobalState &gstate) {
	if (read_position != read_size) {
		// Still data left, read it
		
	}

	AllocatedData buffer;
	if (!current_buffer_handle) {
		// First read
		buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
	}
	if (read_position == read_size) {
		// Fetch new read indices
		auto &file_handle = gstate.json_reader->GetFileHandle();
		{
			lock_guard<mutex> guard(gstate.lock);
			read_size = file_handle.GetPositionAndSize(read_position, gstate.buffer_capacity);
			batch_index = gstate.batch_index++;
		}
		if (read_size == 0) {
			return 0;
		}

	}

	//

	return 0;
}

} // namespace duckdb
