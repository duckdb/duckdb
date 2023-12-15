#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "utf8proc.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct AsciiOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		auto str = input.GetData();
		if (Utf8Proc::Analyze(str, input.GetSize()) == UnicodeType::ASCII) {
			return str[0];
		}
		int utf8_bytes = 4;
		return Utf8Proc::UTF8ToCodepoint(str, utf8_bytes);
	}
};

ScalarFunction ASCIIFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
	                      ScalarFunction::UnaryFunction<string_t, int32_t, AsciiOperator>);
}

} // namespace duckdb
