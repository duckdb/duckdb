#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline string_t GetType(yyjson_val *val, Vector &result) {
	return StringVector::AddString(result, JSONCommon::ValTypeToString(val));
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::UnaryExecute<string_t>(args, state, result, GetType);
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::BinaryExecute<string_t>(args, state, result, GetType);
}

static void ManyTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONCommon::ExecuteMany<string_t>(args, state, result, GetType);
}

CreateScalarFunctionInfo JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	set.AddFunction(ScalarFunction({LogicalType::JSON}, LogicalType::VARCHAR, UnaryTypeFunction, false, false, nullptr,
	                               nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::VARCHAR}, LogicalType::VARCHAR, BinaryTypeFunction,
	                               false, false, JSONReadFunctionData::Bind, nullptr, nullptr));
	set.AddFunction(ScalarFunction({LogicalType::JSON, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction, false, false,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr));

	return CreateScalarFunctionInfo(move(set));
}

} // namespace duckdb
