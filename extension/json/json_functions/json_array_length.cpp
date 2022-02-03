#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline bool GetArrayLength(yyjson_val *val, uint64_t &result) {
	if (val) {
		result = yyjson_arr_size(val);
	}
	return val;
}

static void UnaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::UnaryJSONReadFunction<uint64_t>(args, state, result, GetArrayLength);
}

static void BinaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryJSONReadFunction<uint64_t>(args, state, result, GetArrayLength);
}

CreateScalarFunctionInfo JSONFunctions::GetArrayLengthFunction() {
	ScalarFunctionSet set("json_array_length");
	set.AddFunction(ScalarFunction({LogicalType::JSON}, LogicalType::UBIGINT, UnaryArrayLengthFunction, false, nullptr,
	                               nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::UBIGINT,
	                               BinaryArrayLengthFunction, false, JSONReadFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(move(set));
}

} // namespace duckdb
