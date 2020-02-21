#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

static void strreverse(const char *input, index_t n, char *output) {
	index_t bytes = 0;

	output[n] = 0;

	while (*input) {
		if (!(*input & 0x80)) { // !*input & 0b10000000
			bytes = 1;
		} else if ((*input & 0xe0) == 0xc0) { // (*input & 0b1110_0000 == 0b1100_0000)
			bytes = 2;
		} else if ((*input & 0xf0) == 0xe0) { // (*input & 0b1111_0000 == 0b1110_0000)
			bytes = 3;
		} else if ((*input & 0xf8) == 0xf0) { // (*input & 0b1111_1000 == 0b1111_0000)
			bytes = 4;
		} else {
			assert(false);
		}

		memcpy(&output[n - bytes], input, bytes);
		input += bytes;
		n -= bytes;
	}
}

static void reverse_chunk_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	assert(args.data[0].type == TypeId::VARCHAR);

	index_t current_len = 0;
	unique_ptr<char[]> output;
	UnaryExecutor::Execute<const char*, const char*, true>(args.data[0], result, [&](const char *input) {
		index_t input_length = strlen(input);
		index_t required_len = input_length + 1;
		if (required_len > current_len) {
			current_len = required_len + 1;
			output = unique_ptr<char[]>{new char[current_len]};
		}
		assert(input_length < current_len);
		strreverse(input, input_length, output.get());

		return result.AddString(output.get());
	});
}

void ReverseFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("reverse", {SQLType::VARCHAR}, SQLType::VARCHAR, reverse_chunk_function));
}

} // namespace duckdb
