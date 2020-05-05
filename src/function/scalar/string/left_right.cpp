#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "util.h"

#include <string.h>
#include <ctype.h>
#include <algorithm>	// std::max and std::min

#include <iostream>

using namespace std;

namespace duckdb {

static string_t left_scalar_function(Vector &result, const string_t str, int64_t pos) {
	idx_t current_len = 0;
	unique_ptr<char[]> output;

	if (pos >= 0) {
		return substring_scalar_function(result, str, 1, pos, output, current_len);
	}

	int64_t num_characters = StringLengthOperator::Operation<string_t, int64_t>(str);
	pos = std::max(int64_t(0), num_characters + pos);
	return substring_scalar_function(result, str, 1, pos, output, current_len);
}

static void left_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 2 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT64);
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(str_vec, pos_vec, result, args.size(),
		[&](string_t str, int64_t pos) {
			return left_scalar_function(result, str, pos);
		}
	);
}

void LeftFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("left", {SQLType::VARCHAR, SQLType::BIGINT}, SQLType::VARCHAR, left_function));
}

}
