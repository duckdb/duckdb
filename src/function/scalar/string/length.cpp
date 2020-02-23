#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

struct StringLengthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		int64_t length = 0;
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		for(index_t i = 0; i < input_length; i++) {
			length += (input_data[i] & 0xC0) != 0x80;
		}
		return length;
	}
};

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("length", {SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator, true>));
}

} // namespace duckdb
