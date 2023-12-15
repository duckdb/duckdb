#include "duckdb/core_functions/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

namespace duckdb {

//! Fast ASCII string reverse, returns false if the input data is not ascii
static bool StrReverseASCII(const char *input, idx_t n, char *output) {
	for (idx_t i = 0; i < n; i++) {
		if (input[i] & 0x80) {
			// non-ascii character
			return false;
		}
		output[n - i - 1] = input[i];
	}
	return true;
}

//! Unicode string reverse using grapheme breakers
static void StrReverseUnicode(const char *input, idx_t n, char *output) {
	utf8proc_grapheme_callback(input, n, [&](size_t start, size_t end) {
		memcpy(output + n - end, input + start, end - start);
		return true;
	});
}

struct ReverseOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();

		auto target = StringVector::EmptyString(result, input_length);
		auto target_data = target.GetDataWriteable();
		if (!StrReverseASCII(input_data, input_length, target_data)) {
			StrReverseUnicode(input_data, input_length, target_data);
		}
		target.Finalize();
		return target;
	}
};

static void ReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, ReverseOperator>(args.data[0], result, args.size());
}

ScalarFunction ReverseFun::GetFunction() {
	return ScalarFunction("reverse", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ReverseFunction);
}

} // namespace duckdb
