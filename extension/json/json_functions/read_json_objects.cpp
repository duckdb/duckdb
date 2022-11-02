#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static void ReadJSONObjectsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	D_ASSERT(output.ColumnCount() == 1);
	D_ASSERT(output.data[0].GetType() == LogicalTypeId::VARCHAR);

	auto &bind_data = (JSONScanData &)*data_p.bind_data;
	auto &gstate = (JSONScanGlobalState &)*data_p.global_state;
	auto &lstate = (JSONScanLocalState &)*data_p.local_state;

	if (!lstate.initialized) {
		lstate.local_buffer = gstate.json_reader->AllocateBuffer();
		lstate.initialized = true;
	}
}

TableFunction GetReadJSONObjectsTableFunction(bool list_parameter) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction function({parameter}, ReadJSONObjectsFunction, JSONScanData::Bind, JSONScanGlobalState::Init,
	                       JSONScanLocalState::Init);

	// TODO
	//	function.serialize = CSVReaderSerialize;
	//	function.deserialize = CSVReaderDeserialize;

	// TODO
	//	function.named_parameters[""] = LogicalType;

	return function;
}

CreateTableFunctionInfo JSONFunctions::GetReadJSONObjectsFunction() {
	TableFunctionSet function_set("read_json_objects");
	function_set.AddFunction(GetReadJSONObjectsTableFunction(false));
	function_set.AddFunction(GetReadJSONObjectsTableFunction(true));
	return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb
