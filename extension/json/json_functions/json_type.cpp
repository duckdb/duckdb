#include "json_executors.hpp"

namespace duckdb {

static inline string_t GetType(yyjson_val *val, yyjson_alc *alc, Vector &result) {
	return JSONCommon::ValTypeToStringT<yyjson_val>(val);
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

CreateScalarFunctionInfo JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	set.AddFunction(ScalarFunction({JSONCommon::JSONType()}, LogicalType::VARCHAR, UnaryTypeFunction, nullptr, nullptr,
	                               nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({JSONCommon::JSONType(), LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               BinaryTypeFunction, JSONReadFunctionData::Bind, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({JSONCommon::JSONType(), LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));

	return CreateScalarFunctionInfo(std::move(set));
}

} // namespace duckdb
