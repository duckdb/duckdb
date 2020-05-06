#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <ctype.h>
#include <algorithm>

using namespace std;

namespace duckdb {

static string_t left_scalar_function(Vector &result, const string_t str, int64_t pos, unique_ptr<char[]> &output,
                                     idx_t &current_len) {
	if (pos >= 0) {
		return SubstringFun::substring_scalar_function(result, str, 1, pos, output, current_len);
	}

	int64_t num_characters = LengthFun::Length<string_t, int64_t>(str);
	pos = std::max(int64_t(0), num_characters + pos);
	return SubstringFun::substring_scalar_function(result, str, 1, pos, output, current_len);
}

static void left_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 2 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT64);
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];
	idx_t current_len = 0;
	unique_ptr<char[]> output;

	BinaryExecutor::Execute<string_t, int64_t, string_t, true>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return left_scalar_function(result, str, pos, output, current_len); });
}

void LeftFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("left", {SQLType::VARCHAR, SQLType::BIGINT}, SQLType::VARCHAR, left_function));
}

static string_t right_scalar_function(Vector &result, const string_t str, int64_t pos, unique_ptr<char[]> &output,
                                      idx_t &current_len) {
	int64_t num_characters = LengthFun::Length<string_t, int64_t>(str);
	if (pos >= 0) {
		int64_t len = std::min(num_characters, pos);
		int64_t start = num_characters - len + 1;
		return SubstringFun::substring_scalar_function(result, str, start, len, output, current_len);
	}

	int64_t len = num_characters - std::min(num_characters, -pos);
	int64_t start = num_characters - len + 1;
	return SubstringFun::substring_scalar_function(result, str, start, len, output, current_len);
}

static void right_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 2 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT64);
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];
	idx_t current_len = 0;
	unique_ptr<char[]> output;

	BinaryExecutor::Execute<string_t, int64_t, string_t, true>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return right_scalar_function(result, str, pos, output, current_len); });
}

void RightFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("right", {SQLType::VARCHAR, SQLType::BIGINT}, SQLType::VARCHAR, right_function));
}

} // namespace duckdb
