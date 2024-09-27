#include "json_executors.hpp"

namespace duckdb {

static inline bool JSONExists(yyjson_val *val, yyjson_alc *, Vector &, ValidityMask &, idx_t) {
	return val;
}

static void BinaryExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<bool, false>(args, state, result, JSONExists);
}

static void ManyExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<bool, false>(args, state, result, JSONExists);
}

static void GetExistsFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::BOOLEAN, BinaryExistsFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::BOOLEAN), ManyExistsFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetExistsFunction() {
	ScalarFunctionSet set("json_exists");
	GetExistsFunctionsInternal(set, LogicalType::VARCHAR);
	GetExistsFunctionsInternal(set, LogicalType::JSON());
	return set;
}

} // namespace duckdb
