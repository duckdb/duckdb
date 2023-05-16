#include "duckdb/core_functions/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "utf8proc.hpp"
#include "utf8proc_wrapper.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

static string_t TranslateScalarFunction(const string_t &haystack, const string_t &needle, const string_t &thread,
                                        vector<char> &result) {
	// Get information about the haystack, the needle and the "thread"
	auto input_haystack = haystack.GetData();
	auto size_haystack = haystack.GetSize();

	auto input_needle = needle.GetData();
	auto size_needle = needle.GetSize();

	auto input_thread = thread.GetData();
	auto size_thread = thread.GetSize();

	// Reuse the buffer
	result.clear();
	result.reserve(size_haystack);

	idx_t i = 0, j = 0;
	int sz = 0, c_sz = 0;

	// Character to be replaced
	unordered_map<int32_t, int32_t> to_replace;
	while (i < size_needle && j < size_thread) {
		auto codepoint_needle = Utf8Proc::UTF8ToCodepoint(input_needle, sz);
		input_needle += sz;
		i += sz;
		auto codepoint_thread = Utf8Proc::UTF8ToCodepoint(input_thread, sz);
		input_thread += sz;
		j += sz;
		// Ignore unicode character that is existed in to_replace
		if (to_replace.count(codepoint_needle) == 0) {
			to_replace[codepoint_needle] = codepoint_thread;
		}
	}

	// Character to be deleted
	unordered_set<int32_t> to_delete;
	while (i < size_needle) {
		auto codepoint_needle = Utf8Proc::UTF8ToCodepoint(input_needle, sz);
		input_needle += sz;
		i += sz;
		// Add unicode character that will be deleted
		if (to_replace.count(codepoint_needle) == 0) {
			to_delete.insert(codepoint_needle);
		}
	}

	char c[5] = {'\0', '\0', '\0', '\0', '\0'};
	for (i = 0; i < size_haystack; i += sz) {
		auto codepoint_haystack = Utf8Proc::UTF8ToCodepoint(input_haystack, sz);
		if (to_replace.count(codepoint_haystack) != 0) {
			Utf8Proc::CodepointToUtf8(to_replace[codepoint_haystack], c_sz, c);
			result.insert(result.end(), c, c + c_sz);
		} else if (to_delete.count(codepoint_haystack) == 0) {
			result.insert(result.end(), input_haystack, input_haystack + sz);
		}
		input_haystack += sz;
	}

	return string_t(result.data(), result.size());
}

static void TranslateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &haystack_vector = args.data[0];
	auto &needle_vector = args.data[1];
	auto &thread_vector = args.data[2];

	vector<char> buffer;
	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    haystack_vector, needle_vector, thread_vector, result, args.size(),
	    [&](string_t input_string, string_t needle_string, string_t thread_string) {
		    return StringVector::AddString(result,
		                                   TranslateScalarFunction(input_string, needle_string, thread_string, buffer));
	    });
}

ScalarFunction TranslateFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      TranslateFunction);
}

} // namespace duckdb
