#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline uint64_t GetArrayLength(yyjson_val *val, Vector &result) {
	return yyjson_arr_size(val);
}

static void UnaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::UnaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void BinaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void ManyArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::ExecuteMany<uint64_t>(args, state, result, GetArrayLength);
}

CreateScalarFunctionInfo JSONFunctions::GetArrayLengthFunction() {
	ScalarFunctionSet set("json_array_length");
	set.AddFunction(ScalarFunction({LogicalType::JSON}, LogicalType::UBIGINT, UnaryArrayLengthFunction));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::UBIGINT,
	                               BinaryArrayLengthFunction, JSONReadFunctionData::Bind));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::UBIGINT), ManyArrayLengthFunction,
	                               JSONReadManyFunctionData::Bind));

	return CreateScalarFunctionInfo(move(set));
}

} // namespace duckdb
