#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

struct StringLengthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		int64_t length = 0;
		for (index_t str_idx = 0; input[str_idx]; str_idx++) {
			length += (input[str_idx] & 0xC0) != 0x80;
		}
		return length;
	}
};

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("length", {SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<const char *, int64_t, StringLengthOperator, true>));
}

} // namespace duckdb
