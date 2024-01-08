#include "json_executors.hpp"

namespace duckdb {

static void ValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYAlc();
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(inputs, result, args.size(), [&](string_t input) {
		return JSONCommon::ReadDocumentUnsafe(input, JSONCommon::READ_FLAG, alc);
	});
}

static void GetValidFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction("json_valid", {input_type}, LogicalType::BOOLEAN, ValidFunction, nullptr, nullptr,
	                               nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetValidFunction() {
	ScalarFunctionSet set("json_valid");
	GetValidFunctionInternal(set, LogicalType::VARCHAR);
	GetValidFunctionInternal(set, LogicalType::JSON());

	return set;
}

} // namespace duckdb
