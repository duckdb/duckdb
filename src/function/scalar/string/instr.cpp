#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>

using namespace std;

namespace duckdb {

static int64_t instr(string_t haystack, string_t needle);

struct InstrOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return instr(left, right);
	}
};

static int64_t instr(string_t haystack, string_t needle) {
	int64_t string_position = 0;

	// Getting information about the needle and the haystack
	auto input_haystack = haystack.GetData();
	auto input_needle = needle.GetData();
	auto size_haystack = haystack.GetSize();
	auto size_needle = needle.GetSize();

	// Needle needs something to proceed
	if (size_needle > 0) {
		// Haystack should be bigger or equal size to the needle
		while (size_haystack >= size_needle) {
			// Increment and check continuation bytes: bit 7 should be set and 6 unset
			string_position += (input_haystack[0] & 0xC0) != 0x80;
			// Compare Needle to the Haystack
			if ((memcmp(input_haystack, input_needle, size_needle) == 0)) {
				return string_position;
			}
			size_haystack--;
			input_haystack++;
		}

		// Did not find the needle
		string_position = 0;
	}
	return string_position;
}

void InstrFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("instr",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
	                               SQLType::BIGINT,                      // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator, true>));
}

} // namespace duckdb
