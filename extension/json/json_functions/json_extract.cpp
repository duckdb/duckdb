#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline string_t ExtractFromVal(yyjson_val *val, Vector &result) {
	return JSONCommon::WriteVal(val, result);
}

static inline string_t ExtractStringFromVal(yyjson_val *val, Vector &result) {
	return yyjson_is_str(val) ? StringVector::AddString(result, unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val))
	                          : JSONCommon::WriteVal(val, result);
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryExecute<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::ExecuteMany<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryExecute<string_t>(args, state, result, ExtractStringFromVal);
}

static void ExtractStringManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::ExecuteMany<string_t>(args, state, result, ExtractStringFromVal);
}

CreateScalarFunctionInfo JSONFunctions::GetExtractFunction() {
	// Generic extract function
	ScalarFunctionSet set("json_extract");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::JSON, ExtractFunction,
	                               JSONReadFunctionData::Bind));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::JSON), ExtractManyFunction,
	                               JSONReadManyFunctionData::Bind));

	return CreateScalarFunctionInfo(set);
}

CreateScalarFunctionInfo JSONFunctions::GetExtractStringFunction() {
	// String extract function
	ScalarFunctionSet set("json_extract_string");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               ExtractStringFunction, JSONReadFunctionData::Bind));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ExtractStringManyFunction,
	                               JSONReadManyFunctionData::Bind));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
