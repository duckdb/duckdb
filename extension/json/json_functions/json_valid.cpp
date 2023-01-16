#include "json_executors.hpp"

namespace duckdb {

static void ValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYJSONAllocator();
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(inputs, result, args.size(), [&](string_t input) {
		return !JSONCommon::ReadDocumentUnsafe(input, JSONCommon::BASE_READ_FLAG, alc).IsNull();
	});
}

CreateScalarFunctionInfo JSONFunctions::GetValidFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_valid", {JSONCommon::JSONType()}, LogicalType::BOOLEAN,
	                                               ValidFunction, nullptr, nullptr, nullptr,
	                                               JSONFunctionLocalState::Init));
}

} // namespace duckdb
