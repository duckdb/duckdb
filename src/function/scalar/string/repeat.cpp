#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <string.h>
#include <ctype.h>

namespace duckdb {

static string_t RepeatScalarFunction(const string_t &str, const int64_t cnt, vector<char> &result) {
	// Get information about the repeated string
	auto input_str = str.GetDataUnsafe();
	auto size_str = str.GetSize();

	//  Reuse the buffer
	result.clear();
	for (auto remaining = cnt; remaining-- > 0;) {
		result.insert(result.end(), input_str, input_str + size_str);
	}

	return string_t(result.data(), result.size());
}

static void RepeatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vector = args.data[0];
	auto &cnt_vector = args.data[1];

	vector<char> buffer;
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vector, cnt_vector, result, args.size(), [&](string_t str, int64_t cnt) {
		    return StringVector::AddString(result, RepeatScalarFunction(str, cnt, buffer));
	    });
}

void RepeatFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("repeat",                                    // name of the function
	                               {LogicalType::VARCHAR, LogicalType::BIGINT}, // argument list
	                               LogicalType::VARCHAR,                        // return type
	                               RepeatFunction));                            // pointer to function implementation
}

} // namespace duckdb
