#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool GetType(yyjson_val *val, string_t &result) {
	auto type_string = JSONCommon::ValTypeToString(val);
	if (type_string) {
		result = string_t(type_string);
	}
	return type_string;
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::UnaryJSONReadFunction<string_t>(args, state, result, GetType);
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<string_t>(args, state, result, GetType);
}

static void ManyTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::JSONReadManyFunction<string_t>(args, state, result, GetType);
}

CreateScalarFunctionInfo JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	set.AddFunction(
	    ScalarFunction({LogicalType::JSON}, LogicalType::VARCHAR, UnaryTypeFunction, false, nullptr, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::VARCHAR, BinaryTypeFunction,
	                               false, JSONReadFunctionData::Bind, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(move(set));
}

} // namespace duckdb
