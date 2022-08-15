#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static void ValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(
	    inputs, result, args.size(), [&](string_t input) { return !JSONCommon::ReadDocumentUnsafe(input).IsNull(); });
}

CreateScalarFunctionInfo JSONFunctions::GetValidFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("json_valid", {LogicalType::JSON}, LogicalType::BOOLEAN, ValidFunction));
}

} // namespace duckdb
