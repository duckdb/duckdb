#include "json_common.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

unique_ptr<FunctionData> ReadJSONObjectsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(JSONCommon::JSONType());
	names.emplace_back("json");
	return JSONScanData::Bind(context, input);
}

static void ReadJSONObjectsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(output.ColumnCount() == 1);
	D_ASSERT(JSONCommon::LogicalTypeIsJSON(output.data[0].GetType()));
	auto &gstate = ((JSONGlobalTableFunctionState &)*data_p.global_state).state;
	auto &lstate = ((JSONLocalTableFunctionState &)*data_p.local_state).state;

	// Fetch next lines
	const auto count = lstate.ReadNext(gstate);
	const auto lines = lstate.lines;
	const auto objects = lstate.objects;

	// Create the strings without copying them
	auto strings = FlatVector::GetData<string_t>(output.data[0]);
	auto &validity = FlatVector::Validity(output.data[0]);
	for (idx_t i = 0; i < count; i++) {
		if (objects[i]) {
			strings[i] = string_t(lines[i].pointer, lines[i].size);
		} else {
			validity.SetInvalid(i);
		}
	}

	output.SetCardinality(count);
}

TableFunction GetReadJSONObjectsTableFunction(bool list_parameter, shared_ptr<JSONScanInfo> function_info) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction table_function({parameter}, ReadJSONObjectsFunction, ReadJSONObjectsBind,
	                             JSONGlobalTableFunctionState::Init, JSONLocalTableFunctionState::Init);
	JSONScan::TableFunctionDefaults(table_function);
	table_function.function_info = std::move(function_info);

	return table_function;
}

CreateTableFunctionInfo JSONFunctions::GetReadJSONObjectsFunction() {
	TableFunctionSet function_set("read_json_objects");
	auto function_info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::UNSTRUCTURED);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo JSONFunctions::GetReadNDJSONObjectsFunction() {
	TableFunctionSet function_set("read_ndjson_objects");
	auto function_info = make_shared<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::NEWLINE_DELIMITED);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb
