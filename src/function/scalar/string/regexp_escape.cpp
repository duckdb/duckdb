#include "duckdb/function/scalar/string_functions.hpp"
#include "re2/re2.h"

namespace duckdb {

namespace {

struct EscapeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE &input, Vector &result) {
		auto escaped_pattern = RE2::QuoteMeta(input.GetString());
		return StringVector::AddString(result, escaped_pattern);
	}
};

void RegexpEscapeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, EscapeOperator>(args.data[0], result, args.size());
}

} // namespace

ScalarFunction RegexpEscapeFun::GetFunction() {
	return ScalarFunction("regexp_escape", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RegexpEscapeFunction);
}

} // namespace duckdb
