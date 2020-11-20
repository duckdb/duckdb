#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <ctype.h>
#include <algorithm>

using namespace std;

namespace duckdb {

static string_t left_scalar_function(Vector &result, const string_t str, int64_t pos) {
	if (pos >= 0) {
		return SubstringFun::substring_scalar_function(result, str, 1, pos);
	}

	int64_t num_characters = LengthFun::Length<string_t, int64_t>(str);
	pos = MaxValue<int64_t>(0, num_characters + pos);
	return SubstringFun::substring_scalar_function(result, str, 1, pos);
}

static void left_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t, true>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return left_scalar_function(result, str, pos); });
}

void LeftFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("left", {LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR, left_function));
}

static string_t right_scalar_function(Vector &result, const string_t str, int64_t pos) {
	int64_t num_characters = LengthFun::Length<string_t, int64_t>(str);
	if (pos >= 0) {
		int64_t len = MinValue<int64_t>(num_characters, pos);
		int64_t start = num_characters - len + 1;
		return SubstringFun::substring_scalar_function(result, str, start, len);
	}

	int64_t len = num_characters - MinValue<int64_t>(num_characters, -pos);
	int64_t start = num_characters - len + 1;
	return SubstringFun::substring_scalar_function(result, str, start, len);
}

static void right_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];
	BinaryExecutor::Execute<string_t, int64_t, string_t, true>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return right_scalar_function(result, str, pos); });
}

void RightFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("right", {LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR, right_function));
}

} // namespace duckdb
