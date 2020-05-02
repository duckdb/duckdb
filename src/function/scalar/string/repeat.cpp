#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <algorithm>    // std::max

using namespace std;

namespace duckdb {

static string_t repeat_scalar_function(const string_t& str, const int64_t cnt, vector<char> &result) {
	// Get information about the repeated string
	const auto input_str = str.GetData();
	const auto size_str = str.GetSize();

	//  Reuse the buffer
	result.clear();
	for (auto remaining = cnt; remaining-- > 0;) {
		result.insert(result.end(), input_str, input_str + size_str);
	}

	return string_t(result.data(), result.size());
}

static void repeat_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 2 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT64);
	auto &str_vector = args.data[0];
	auto &cnt_vector = args.data[1];

	vector<char> buffer;
	BinaryExecutor::Execute<string_t, int64_t, string_t>(str_vector, cnt_vector, result, args.size(),
		[&](string_t str, int64_t cnt) {
		    return StringVector::AddString(result,
		                                   repeat_scalar_function(str, cnt, buffer));
		}
	);
}

void RepeatFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("repeat",                            // name of the function
	                               {SQLType::VARCHAR, SQLType::BIGINT}, // argument list
	                               SQLType::VARCHAR,                    // return type
	                               repeat_function));                   // pointer to function implementation
}

} // namespace duckdb
