#include "json_common.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

static void ReadJSONObjectsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(output.ColumnCount() == 1);
	D_ASSERT(JSONCommon::LogicalTypeIsJSON(output.data[0].GetType()));
	auto &gstate = (JSONScanGlobalState &)*data_p.global_state;
	auto &lstate = (JSONScanLocalState &)*data_p.local_state;

	// Fetch next lines
	const auto count = lstate.ReadNext(gstate);
	const auto lines = lstate.lines;

	// Create the strings without copying them
	auto strings = FlatVector::GetData<string_t>(output.data[0]);
	for (idx_t i = 0; i < count; i++) {
		strings[i] = string_t(lines[i].pointer, lines[i].size);
	}

	output.SetCardinality(count);
}

TableFunction GetReadJSONObjectsTableFunction(bool list_parameter, shared_ptr<JSONScanInfo> function_info) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction table_function({parameter}, ReadJSONObjectsFunction, JSONScanData::Bind, JSONScanGlobalState::Init,
	                             JSONScanLocalState::Init);

	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["maximum_object_size"] = LogicalType::UBIGINT;

	table_function.table_scan_progress = JSONScanProgress;
	table_function.get_batch_index = JSONScanGetBatchIndex;

	// TODO:
	//	table_function.serialize = JSONScanSerialize;
	//	table_function.deserialize = JSONScanDeserialize;

	table_function.projection_pushdown = false;
	table_function.filter_pushdown = false;
	table_function.filter_prune = false;

	table_function.function_info = move(function_info);

	return table_function;
}

CreateTableFunctionInfo JSONFunctions::GetReadJSONObjectsFunction() {
	TableFunctionSet function_set("read_json_objects");
	auto function_info = make_shared<JSONScanInfo>(JSONFormat::UNSTRUCTURED, true);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo JSONFunctions::GetReadNDJSONObjectsFunction() {
	TableFunctionSet function_set("read_ndjson_objects");
	auto function_info = make_shared<JSONScanInfo>(JSONFormat::NEWLINE_DELIMITED, true);
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false, function_info));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true, function_info));
	return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb
