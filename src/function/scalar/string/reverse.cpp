#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

//! Fast ASCII string reverse, returns false if the input data is not ascii
static bool strreverse_ascii(const char *input, idx_t n, char *output) {
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
static void strreverse_unicode(const char *input, idx_t n, char *output) {
	utf8proc_grapheme_callback(input, n, [&](size_t start, size_t end) {
		memcpy(output + n - end, input + start, end - start);
		return true;
	});
}

static void reverse_chunk_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	assert(args.data[0].type == TypeId::VARCHAR);

	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();

		auto target = StringVector::EmptyString(result, input_length);
		auto target_data = target.GetData();
		if (!strreverse_ascii(input_data, input_length, target_data)) {
			strreverse_unicode(input_data, input_length, target_data);
		}
		target.Finalize();
		return target;
	});
}

void ReverseFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("reverse", {SQLType::VARCHAR}, SQLType::VARCHAR, reverse_chunk_function));
}

} // namespace duckdb
