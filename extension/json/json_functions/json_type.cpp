#include "json_executors.hpp"

namespace duckdb {

static inline optional<string_t> GetType(yyjson_val *val, yyjson_alc *, Vector &) {
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
	set.AddFunction(ScalarFunction({input_type}, LogicalType::VARCHAR, UnaryTypeFunction, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, BinaryTypeFunction,
	                               JSONReadFunctionData::Bind, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	GetTypeFunctionsInternal(set, LogicalType::VARCHAR);
	GetTypeFunctionsInternal(set, LogicalType::JSON());
	for (auto &func : set.functions) {
		const auto &sig = func.GetSignature();
		if (sig.GetParameterCount() == 1 && sig.GetParameter(0).GetType().IsJSONType()) {
			continue;
		}
		func.SetFallible();
	}
	return set;
}

} // namespace duckdb
