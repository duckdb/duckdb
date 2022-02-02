#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool JSON(yyjson_val *val, string_t &result) {
	if (val) {
		result = JSONCommon::WriteDocument(val);
	}
	return val;
}

static void JSONFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::TemplatedUnaryJSONFunction<string_t>(args, state, result, JSON);
}

CreateScalarFunctionInfo JSONFunctions::GetJSONFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json", {LogicalType::VARCHAR}, LogicalType::JSON, JSONFunction,
	                                               false, nullptr, nullptr, nullptr));
}

} // namespace duckdb
