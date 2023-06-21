#include "json_common.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

unique_ptr<FunctionData> ReadJSONObjectsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<JSONScanData>();
	bind_data->Bind(context, input);

	bind_data->names.emplace_back("json");
	return_types.push_back(JSONCommon::JSONType());
	names.emplace_back("json");

	bind_data->reader_bind =
	    MultiFileReader::BindOptions(bind_data->options.file_options, bind_data->files, return_types, names);

	return std::move(bind_data);
}

static void ReadJSONObjectsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<JSONGlobalTableFunctionState>().state;
	auto &lstate = data_p.local_state->Cast<JSONLocalTableFunctionState>().state;

	// Fetch next lines
	const auto count = lstate.ReadNext(gstate);
	const auto units = lstate.units;
	const auto objects = lstate.values;

	if (!gstate.names.empty()) {
		// Create the strings without copying them
		auto strings = FlatVector::GetData<string_t>(output.data[0]);
		auto &validity = FlatVector::Validity(output.data[0]);
		for (idx_t i = 0; i < count; i++) {
			if (objects[i]) {
				strings[i] = string_t(units[i].pointer, units[i].size);
			} else {
				validity.SetInvalid(i);
			}
		}
	}

	output.SetCardinality(count);

	if (output.size() != 0) {
		MultiFileReader::FinalizeChunk(gstate.bind_data.reader_bind, lstate.GetReaderData(), output);
	}
}

TableFunction GetReadJSONObjectsTableFunction(bool list_parameter, shared_ptr<JSONScanInfo> function_info) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction table_function({parameter}, ReadJSONObjectsFunction, ReadJSONObjectsBind,
	                             JSONGlobalTableFunctionState::Init, JSONLocalTableFunctionState::Init);
	JSONScan::TableFunctionDefaults(table_function);
	table_function.function_info = std::move(function_info);

	return table_function;
}

TableFunctionSet JSONFunctions::GetReadJSONObjectsFunction() {
	TableFunctionSet function_set("read_json_objects");
	auto function_info =
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::ARRAY, JSONRecordType::RECORDS);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return function_set;
}

TableFunctionSet JSONFunctions::GetReadNDJSONObjectsFunction() {
	TableFunctionSet function_set("read_ndjson_objects");
	auto function_info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::NEWLINE_DELIMITED,
	                                               JSONRecordType::RECORDS);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return function_set;
}

TableFunctionSet JSONFunctions::GetReadJSONObjectsAutoFunction() {
	TableFunctionSet function_set("read_json_objects_auto");
	auto function_info =
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::AUTO_DETECT, JSONRecordType::RECORDS);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return function_set;
}

} // namespace duckdb
