#include "json_common.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "duckdb/common/helper.hpp"
#include "json_multi_file_info.hpp"

namespace duckdb {

static void ReadJSONObjectsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<JSONGlobalTableFunctionState>().state;
	auto &lstate = data_p.local_state->Cast<JSONLocalTableFunctionState>().state;
	//
	// // Fetch next lines
	// const auto count = lstate.ReadNext(gstate);
	// auto &scan_state = lstate.GetScanState();
	// const auto units = scan_state.units;
	// const auto objects = scan_state.values;
	//
	// if (!gstate.names.empty()) {
	// 	// Create the strings without copying them
	// 	const auto col_idx = gstate.column_ids[0];
	// 	auto strings = FlatVector::GetData<string_t>(output.data[col_idx]);
	// 	auto &validity = FlatVector::Validity(output.data[col_idx]);
	// 	for (idx_t i = 0; i < count; i++) {
	// 		if (objects[i]) {
	// 			strings[i] = string_t(units[i].pointer, units[i].size);
	// 		} else {
	// 			validity.SetInvalid(i);
	// 		}
	// 	}
	// }
	//
	// output.SetCardinality(count);
	//
	// if (output.size() != 0) {
	// 	MultiFileReader().FinalizeChunk(context, gstate.bind_data.reader_bind, lstate.GetReaderData(), output, nullptr);
	// }
}

TableFunction GetReadJSONObjectsTableFunction(string name, shared_ptr<JSONScanInfo> function_info) {
	MultiFileReaderFunction<JSONMultiFileInfo> table_function(std::move(name));
	JSONScan::TableFunctionDefaults(table_function);
	table_function.function_info = std::move(function_info);
	return static_cast<TableFunction>(table_function);
}

TableFunctionSet JSONFunctions::GetReadJSONObjectsFunction() {
	auto function_info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::AUTO_DETECT,
	                                                   JSONRecordType::RECORDS);
	auto table_function = GetReadJSONObjectsTableFunction("read_json_objects", std::move(function_info));
	return MultiFileReader::CreateFunctionSet(std::move(table_function));
}

TableFunctionSet JSONFunctions::GetReadNDJSONObjectsFunction() {
	auto function_info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::NEWLINE_DELIMITED,
	                                                   JSONRecordType::RECORDS);
	auto table_function = GetReadJSONObjectsTableFunction("read_ndjson_objects", std::move(function_info));
	return MultiFileReader::CreateFunctionSet(std::move(table_function));
}

TableFunctionSet JSONFunctions::GetReadJSONObjectsAutoFunction() {
	auto function_info = make_shared_ptr<JSONScanInfo>(JSONScanType::READ_JSON_OBJECTS, JSONFormat::AUTO_DETECT,
	                                                   JSONRecordType::RECORDS);
	auto table_function = GetReadJSONObjectsTableFunction("read_json_objects_auto", std::move(function_info));
	return MultiFileReader::CreateFunctionSet(std::move(table_function));
}

} // namespace duckdb
