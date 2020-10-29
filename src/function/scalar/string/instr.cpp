#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "utf8proc.hpp"

#include <cstring>

using namespace std;

namespace duckdb {

static int64_t instr(string_t haystack, string_t needle) {
	int64_t string_position = 0;

	// Getting information about the needle and the haystack
	auto haystack_data = haystack.GetString();
	auto needle_data = needle.GetString();
	auto location_data = strstr(haystack_data.c_str(), needle_data.c_str());
	if (location_data) {
		auto str = reinterpret_cast<const utf8proc_uint8_t *>(haystack_data.c_str());
		utf8proc_ssize_t len = location_data - haystack_data.c_str();
		for (++string_position; len > 0; ++string_position) {
			utf8proc_int32_t codepoint;
			const auto bytes = utf8proc_iterate(str, len, &codepoint);
			str += bytes;
			len -= bytes;
		}
	}

	return string_position;
}

struct InstrOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return instr(left, right);
	}
};

void InstrFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("instr",                                      // name of the function
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                               LogicalType::BIGINT,                          // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator, true>));
}

} // namespace duckdb
