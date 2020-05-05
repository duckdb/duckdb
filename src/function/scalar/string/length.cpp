#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

// length returns the size in characters
struct StringLengthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return LengthFun::Length<TA, TR>(input);
	}
};

// strlen returns the size in bytes
struct StrLenOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return input.GetSize();
	}
};

// bitlen returns the size in bits
struct BitLenOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 8 * input.GetSize();
	}
};

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"length", "len"},
	                ScalarFunction({SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator, true>));
	set.AddFunction(ScalarFunction("strlen", {SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator, true>));
	set.AddFunction(ScalarFunction("bit_length", {SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, BitLenOperator, true>));
}

struct UnicodeOperator {
	template <class TA, class TR> static inline TR Operation(const TA &input) {
		const auto str = reinterpret_cast<const utf8proc_uint8_t *>(input.GetData());
		const auto len = input.GetSize();
		utf8proc_int32_t codepoint;
		(void)utf8proc_iterate(str, len, &codepoint);
		return codepoint;
	}
};

void UnicodeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("unicode", {SQLType::VARCHAR}, SQLType::INTEGER,
	                               ScalarFunction::UnaryFunction<string_t, int32_t, UnicodeOperator, true>));
}

} // namespace duckdb
