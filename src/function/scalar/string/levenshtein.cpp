#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

#include <ctype.h>
#include <algorithm>

namespace duckdb {

static int64_t LevenshteinScalarFunction(Vector &result, const string_t str, string_t tgt) {
	int64_t num_characters_str = LengthFun::Length<string_t, int64_t>(str);
	int64_t num_characters_tgt = LengthFun::Length<string_t, int64_t>(tgt);

	if (num_characters_str < 1 || num_characters_tgt < 1) {
		return -1; // too short
	}
 
    //return StringUtil::LevenshteinDistance((const string&)str, (const string&)tgt);
	return -1;
}

static void LevenshteinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return LevenshteinScalarFunction(result, str, tgt); });
}

void LevenshteinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("levenshtein", {
			LogicalType::VARCHAR, 
			LogicalType::VARCHAR
		}, 
		LogicalType::BIGINT, LevenshteinFunction)); // Pointer to function implementation
}

} // namespace duckdb
