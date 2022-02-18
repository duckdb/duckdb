#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool ExtractFromVal(yyjson_val *val, string_t &result) {
	if (val) {
		result = JSONCommon::WriteVal(val);
	}
	return val;
}

static inline bool ExtractStringFromVal(yyjson_val *val, string_t &result) {
	if (val) {
		result = JSONCommon::WriteStringVal(val);
	}
	return val;
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::JSONReadManyFunction<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<string_t>(args, state, result, ExtractStringFromVal);
}

static void ExtractStringManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::JSONReadManyFunction<string_t>(args, state, result, ExtractStringFromVal);
}

CreateScalarFunctionInfo JSONFunctions::GetExtractFunction() {
	// Generic extract function
	ScalarFunctionSet set("json_extract");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::JSON, ExtractFunction, false,
	                               JSONReadFunctionData::Bind, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::JSON), ExtractManyFunction, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(set);
}

CreateScalarFunctionInfo JSONFunctions::GetExtractStringFunction() {
	// String extract function
	ScalarFunctionSet set("json_extract_string");
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               ExtractStringFunction, false, JSONReadFunctionData::Bind, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ExtractStringManyFunction, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
