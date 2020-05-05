#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "utf8proc.hpp"
#include "util.hpp"
#include <algorithm>

#include <iostream>

using namespace std;

namespace duckdb {

static void substring_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 3 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT32 &&
	       args.data[2].type == TypeId::INT32);
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];
	auto &length_vector = args.data[2];

	idx_t current_len = 0;
	unique_ptr<char[]> output;
	TernaryExecutor::Execute<string_t, int, int, string_t>(
	    input_vector, offset_vector, length_vector, result, args.size(),
	    [&](string_t input_string, int offset, int length) {
		    return substring_scalar_function(result, input_string, offset, length, output, current_len);
	    });
}

void SubstringFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"substring", "substr"}, ScalarFunction({SQLType::VARCHAR, SQLType::INTEGER, SQLType::INTEGER},
	                                                        SQLType::VARCHAR, substring_function));
}

} // namespace duckdb
