#include "json_executors.hpp"

namespace duckdb {

static inline string_t GetType(yyjson_val *val, yyjson_alc *alc, Vector &result) {
	return JSONCommon::ValTypeToStringT(val);
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<string_t>(args, state, result, GetType);
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, GetType);
}

static void ManyTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, GetType);
}

static void GetTypeFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::VARCHAR, UnaryTypeFunction, nullptr, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, BinaryTypeFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	GetTypeFunctionsInternal(set, LogicalType::VARCHAR);
	GetTypeFunctionsInternal(set, JSONCommon::JSONType());
	return set;
}

} // namespace duckdb
