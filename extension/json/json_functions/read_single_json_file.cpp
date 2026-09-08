#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/common/mutex.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

//! Bind data of read_single_json_file - the regular JSON scan data plus the single file that is read
struct ReadSingleJSONFileData : public JSONScanData {
	OpenFileInfo file;
};

struct ReadSingleJSONFileGlobalState : public GlobalTableFunctionState {
public:
	ReadSingleJSONFileGlobalState(ClientContext &context, const ReadSingleJSONFileData &json_data)
	    : state(context, json_data, 1),
	      reader(make_shared_ptr<JSONReader>(context, json_data.options, json_data.file)) {
	}

public:
	idx_t MaxThreads() const override {
		if (!state.enable_parallel_scans) {
			return 1;
		}
		return state.json_data.max_threads.IsValid() ? state.json_data.max_threads.GetIndex() : idx_t(MAX_THREADS);
	}

public:
	JSONScanGlobalState state;
	shared_ptr<JSONReader> reader;
	//! Assigning the next part of the file to a thread is done single-threadedly
	mutex lock;
};

struct ReadSingleJSONFileLocalState : public LocalTableFunctionState {
public:
	ReadSingleJSONFileLocalState(ClientContext &context, JSONScanGlobalState &gstate) : state(context, gstate) {
		// the transform options are thread-local
		state.transform_options = gstate.transform_options;
	}

public:
	JSONScanLocalState state;
	//! Whether we have a part of the file assigned to us that we still need to read
	bool scan_initialized = false;
};

static unique_ptr<FunctionData> ReadSingleJSONFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ReadSingleJSONFileData>();
	auto &options = result->options;
	if (input.info) {
		auto &scan_info = input.info->Cast<JSONScanInfo>();
		options.type = scan_info.type;
		options.format = scan_info.format;
		options.record_type = scan_info.record_type;
		options.auto_detect = scan_info.auto_detect;
		if (scan_info.type == JSONScanType::READ_JSON_OBJECTS) {
			// read_json_objects always emits a single JSON column called "json"
			options.sql_type_list.push_back(LogicalType::JSON());
			options.name_list.emplace_back("json");
		}
	}
	for (auto &kv : input.named_parameters) {
		if (JSONScan::ParseOption(context, kv.first, kv.second, options)) {
			continue;
		}
		throw NotImplementedException("Unimplemented option %s", kv.first);
	}
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_single_json_file requires a non-NULL file name");
	}
	result->file = OpenFileInfo(StringValue::Get(input.inputs[0]));

	SimpleMultiFileList file_list(vector<OpenFileInfo> {result->file});
	vector<shared_ptr<BaseUnionData>> union_readers;
	JSONScan::BindSchema(context, *result, file_list, union_readers, false, return_types, names);
	JSONScan::FinalizeBind(*result, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ReadSingleJSONFileInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
	auto &json_data = input.bind_data->Cast<ReadSingleJSONFileData>();
	auto result = make_uniq<ReadSingleJSONFileGlobalState>(context, json_data);
	auto &gstate = result->state;

	// perform projection pushdown - the JSON reader extracts columns by name, so we only need the projected names
	for (idx_t col_idx = 0; col_idx < input.column_indexes.size(); col_idx++) {
		auto &column_index = input.column_indexes[col_idx];
		const auto col_id = column_index.GetPrimaryIndex();
		if (IsVirtualColumn(col_id)) {
			continue;
		}
		gstate.names.push_back(json_data.key_names[col_id]);
		gstate.column_ids.push_back(col_idx);
		gstate.column_indices.push_back(column_index);
		if (!json_data.feature_columns.empty()) {
			gstate.feature_columns.push_back(json_data.feature_columns[col_id]);
		}
	}
	if (gstate.names.size() < json_data.key_names.size()) {
		// if we don't need all columns present in the file we don't error on unseen columns
		gstate.transform_options.error_unknown_key = false;
	}
	if (gstate.enable_parallel_scans) {
		// if we are doing parallel scans we need to open the file here
		result->reader->Initialize(gstate.allocator, gstate.buffer_capacity);
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ReadSingleJSONFileInitLocal(ExecutionContext &context,
                                                                       TableFunctionInitInput &input,
                                                                       GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<ReadSingleJSONFileGlobalState>();
	return make_uniq<ReadSingleJSONFileLocalState>(context.client, gstate.state);
}

static void ReadSingleJSONFileFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<ReadSingleJSONFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleJSONFileLocalState>();
	auto &reader = *gstate.reader;

	while (true) {
		if (!lstate.scan_initialized) {
			lock_guard<mutex> guard(gstate.lock);
			lstate.state.GetScanState().ResetForNextBuffer();
			if (!lstate.state.TryInitializeScan(gstate.state, reader)) {
				// there is nothing left for us to read in this file
				return;
			}
			lstate.scan_initialized = true;
		}
		switch (gstate.state.json_data.options.type) {
		case JSONScanType::READ_JSON:
			ReadJSONFunction(context, reader, gstate.state, lstate.state, output);
			break;
		case JSONScanType::READ_JSON_OBJECTS:
			ReadJSONObjectsFunction(context, reader, gstate.state, lstate.state, output);
			break;
		default:
			throw InternalException("Unsupported scan type for read_single_json_file");
		}
		if (output.size() != 0) {
			return;
		}
		// we have exhausted the part of the file that was assigned to us - try to grab the next part
		lstate.scan_initialized = false;
	}
}

static double ReadSingleJSONFileProgress(ClientContext &context, const FunctionData *bind_data,
                                         const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}
	return global_state->Cast<ReadSingleJSONFileGlobalState>().reader->GetProgress();
}

static unique_ptr<NodeStatistics> ReadSingleJSONFileCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &json_data = bind_data->Cast<ReadSingleJSONFileData>();
	if (!json_data.estimated_cardinality_per_file.IsValid()) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(json_data.estimated_cardinality_per_file.GetIndex());
}

TableFunction JSONFunctions::GetReadSingleJSONFileTableFunction(shared_ptr<JSONScanInfo> function_info) {
	TableFunction table_function("read_single_json_file", {LogicalType::VARCHAR}, ReadSingleJSONFileFunction,
	                             ReadSingleJSONFileBind, ReadSingleJSONFileInitGlobal, ReadSingleJSONFileInitLocal);
	JSONScan::TableFunctionDefaults(table_function);
	JSONScan::AddReadJSONParameters(table_function);
	JSONScan::AddAutoDetectParameters(table_function);
	table_function.table_scan_progress = ReadSingleJSONFileProgress;
	table_function.cardinality = ReadSingleJSONFileCardinality;
	table_function.function_info = std::move(function_info);
	return table_function;
}

static shared_ptr<JSONScanInfo> ReadJSONScanInfo() {
	return make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT, JSONRecordType::AUTO_DETECT,
	                                     true);
}

TableFunctionSet JSONFunctions::GetReadSingleJSONFileFunction() {
	TableFunctionSet function_set("read_single_json_file");
	function_set.AddFunction(GetReadSingleJSONFileTableFunction(ReadJSONScanInfo()));
	return function_set;
}

TableFunctionSet JSONFunctions::GetReadJSONNewFunction() {
	// wrap the single-file JSON reader into a multi-file table function
	auto single_file_function = GetReadSingleJSONFileTableFunction(ReadJSONScanInfo());
	return TableFunctionMultiFileWrapper::CreateFunctionSet(std::move(single_file_function), "read_json_new",
	                                                        FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "json"),
	                                                        "JSON");
}

} // namespace duckdb
