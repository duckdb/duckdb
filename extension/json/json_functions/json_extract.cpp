#include "json_executors.hpp"

namespace duckdb {

static inline string_t ExtractFromVal(yyjson_val *val, yyjson_alc *alc, Vector &result) {
	return JSONCommon::WriteVal<yyjson_val>(val, alc);
}

static inline string_t ExtractStringFromVal(yyjson_val *val, yyjson_alc *alc, Vector &result) {
	return yyjson_is_str(val) ? StringVector::AddString(result, unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val))
	                          : JSONCommon::WriteVal<yyjson_val>(val, alc);
}

static void ExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, ExtractFromVal);
}

static void ExtractStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, ExtractStringFromVal);
}

static void ExtractStringManyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, ExtractStringFromVal);
}

CreateScalarFunctionInfo JSONFunctions::GetExtractFunction() {
	// Generic extract function
	ScalarFunctionSet set("json_extract");
	set.AddFunction(ScalarFunction({JSONCommon::JSONType(), LogicalType::VARCHAR}, JSONCommon::JSONType(),
	                               ExtractFunction, JSONReadFunctionData::Bind, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({JSONCommon::JSONType(), LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(JSONCommon::JSONType()), ExtractManyFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));

	return CreateScalarFunctionInfo(set);
}

CreateScalarFunctionInfo JSONFunctions::GetExtractStringFunction() {
	// String extract function
	ScalarFunctionSet set("json_extract_string");
	set.AddFunction(ScalarFunction({JSONCommon::JSONType(), LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               ExtractStringFunction, JSONReadFunctionData::Bind, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({JSONCommon::JSONType(), LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ExtractStringManyFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
