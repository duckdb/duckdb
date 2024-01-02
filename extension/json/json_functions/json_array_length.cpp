#include "json_executors.hpp"

namespace duckdb {

static inline uint64_t GetArrayLength(yyjson_val *val, yyjson_alc *alc, Vector &result) {
	return yyjson_arr_size(val);
}

static void UnaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void BinaryArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<uint64_t>(args, state, result, GetArrayLength);
}

static void ManyArrayLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<uint64_t>(args, state, result, GetArrayLength);
}

static void GetArrayLengthFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::UBIGINT, UnaryArrayLengthFunction, nullptr, nullptr,
	                               nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::UBIGINT, BinaryArrayLengthFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::UBIGINT), ManyArrayLengthFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetArrayLengthFunction() {
	ScalarFunctionSet set("json_array_length");
	GetArrayLengthFunctionsInternal(set, LogicalType::VARCHAR);
	GetArrayLengthFunctionsInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
