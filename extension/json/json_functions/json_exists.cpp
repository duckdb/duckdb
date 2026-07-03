#include "json_executors.hpp"

namespace duckdb {

static inline optional<bool> JSONExists(yyjson_val *val, yyjson_alc *, Vector &) {
	return val != nullptr;
}

static void BinaryExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<bool, false>(args, state, result, JSONExists);
}

static void ManyExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<bool, false>(args, state, result, JSONExists);
}

static void GetExistsFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::BOOLEAN, BinaryExistsFunction,
	                               JSONReadFunctionData::Bind, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::BOOLEAN), ManyExistsFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetExistsFunction() {
	ScalarFunctionSet set("json_exists");
	GetExistsFunctionsInternal(set, LogicalType::VARCHAR);
	GetExistsFunctionsInternal(set, LogicalType::JSON());
	for (auto &func : set.functions) {
		func.SetFallible();
	}
	return set;
}

} // namespace duckdb
