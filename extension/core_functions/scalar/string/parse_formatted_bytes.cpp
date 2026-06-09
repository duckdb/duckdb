#include "duckdb/function/scalar_function.hpp"

#include "core_functions/scalar/string_functions.hpp"

namespace duckdb {
static void ParseFormattedBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto arg0 = args.data[0];
	UnaryExecutor::Execute<string_t, idx_t>(arg0, result, args.size(), [&](string_t str) {
		// Invalid input exceptions thrown from ParseFormattedBytes won't be handled but will be thrown as is
		return StringUtil::ParseFormattedBytes(str.GetString());
	});
}

ScalarFunction ParseFormattedBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, ParseFormattedBytesFunction);
}
} // namespace duckdb
