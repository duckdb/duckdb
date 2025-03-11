#include "json_common.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "duckdb/common/helper.hpp"
#include "json_multi_file_info.hpp"

namespace duckdb {

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
