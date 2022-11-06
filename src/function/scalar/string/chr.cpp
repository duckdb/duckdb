#include "duckdb/function/scalar/string_functions.hpp"
#include "utf8proc.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ChrOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		char c[5] = {'\0', '\0', '\0', '\0', '\0'};
		int utf8_bytes;
		if (input < 0 || !Utf8Proc::CodepointToUtf8(input, utf8_bytes, &c[0])) {
			throw InvalidInputException("Invalid UTF8 Codepoint %d", input);
		}
		return string_t(&c[0], utf8_bytes);
	}
};

void CHR::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction chr("chr", {LogicalType::INTEGER}, LogicalType::VARCHAR,
	                   ScalarFunction::UnaryFunction<int32_t, string_t, ChrOperator>);
	set.AddFunction(chr);
}

} // namespace duckdb
