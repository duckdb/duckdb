#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/strnstrn.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

static int64_t instr(string_t haystack, string_t needle) {
	int64_t string_position = 0;

	// Getting information about the needle and the haystack
	auto location_data =
	    strnstrn(haystack.GetDataUnsafe(), needle.GetDataUnsafe(), haystack.GetSize(), needle.GetSize());
	if (location_data) {
		auto str = reinterpret_cast<const utf8proc_uint8_t *>(haystack.GetDataUnsafe());
		utf8proc_ssize_t len = location_data - haystack.GetDataUnsafe();
		assert(len <= (utf8proc_ssize_t)haystack.GetSize());
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
