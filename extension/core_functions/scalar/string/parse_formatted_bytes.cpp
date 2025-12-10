#include "duckdb/function/scalar_function.hpp"

#include <core_functions/scalar/string_functions.hpp>

namespace duckdb {
static void PraseFormattedBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto arg0 = args.data[0];
	UnaryExecutor::Execute<string_t, idx_t>(arg0, result, args.size(), [&](string_t str) {
		// Invalid input exceptions thrown from ParseMemoryLimit won't be handled but will be thrown as is
		return StringUtil::ParseFormattedBytes(str.GetString());
	});
}

static void TryParseFormattedBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto arg0 = args.data[0];

	UnaryExecutor::ExecuteWithNulls<string_t, idx_t>(arg0, result, args.size(), [&](string_t str, ValidityMask &mask, idx_t index) {
		try {
			return StringUtil::ParseFormattedBytes(str.GetString());
		} catch (const Exception &) {
			mask.SetInvalid(index);
			return static_cast<idx_t>(0);
		}
	});
}

ScalarFunction ParseFormattedBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, PraseFormattedBytesFunction);
}

ScalarFunction TryParseFormattedBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, TryParseFormattedBytesFunction);
}

} // namespace duckdb
