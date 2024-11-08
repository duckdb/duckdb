#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

struct LeftRightUnicode {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Length<TA, TR>(input);
	}

	static string_t Substring(Vector &result, string_t input, int64_t offset, int64_t length) {
		return SubstringUnicode(result, input, offset, length);
	}
};

struct LeftRightGrapheme {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return GraphemeCount<TA, TR>(input);
	}

	static string_t Substring(Vector &result, string_t input, int64_t offset, int64_t length) {
		return SubstringGrapheme(result, input, offset, length);
	}
};

template <class OP>
static string_t LeftScalarFunction(Vector &result, const string_t str, int64_t pos) {
	if (pos >= 0) {
		return OP::Substring(result, str, 1, pos);
	}

	int64_t num_characters = OP::template Operation<string_t, int64_t>(str);
	pos = MaxValue<int64_t>(0, num_characters + pos);
	return OP::Substring(result, str, 1, pos);
}

template <class OP>
static void LeftFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return LeftScalarFunction<OP>(result, str, pos); });
}

ScalarFunction LeftFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      LeftFunction<LeftRightUnicode>);
}

ScalarFunction LeftGraphemeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      LeftFunction<LeftRightGrapheme>);
}

template <class OP>
static string_t RightScalarFunction(Vector &result, const string_t str, int64_t pos) {
	int64_t num_characters = OP::template Operation<string_t, int64_t>(str);
	if (pos >= 0) {
		int64_t len = MinValue<int64_t>(num_characters, pos);
		int64_t start = num_characters - len + 1;
		return OP::Substring(result, str, start, len);
	}

	int64_t len = 0;
	if (pos != std::numeric_limits<int64_t>::min()) {
		len = num_characters - MinValue<int64_t>(num_characters, -pos);
	}
	int64_t start = num_characters - len + 1;
	return OP::Substring(result, str, start, len);
}

template <class OP>
static void RightFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return RightScalarFunction<OP>(result, str, pos); });
}

ScalarFunction RightFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      RightFunction<LeftRightUnicode>);
}

ScalarFunction RightGraphemeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      RightFunction<LeftRightGrapheme>);
}

} // namespace duckdb
