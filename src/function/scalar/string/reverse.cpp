#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

typedef void (*str_function)(const char *input, char *output);

static void strreverse(const char *input, char *output) {
	size_t n = strlen(input); // the size of the string in bytes
	size_t bytes = 0;

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

template <str_function REVERSE_FUNCTION> static void reverse_function(Vector &input, Vector &result) {
	assert(input.type == TypeId::VARCHAR);

	result.nullmask = input.nullmask;

	auto result_data = (const char **)result.GetData();
	auto input_data = (const char **)input.GetData();

	index_t current_len = 0;
	unique_ptr<char[]> output;
	VectorOperations::Exec(input, [&](index_t i, index_t k) {
		if (input.nullmask[i]) {
			return;
		}
		// if (!has_stats) {
		// no stats available, might need to reallocate
		index_t required_len = strlen(input_data[i]) + 1;
		if (required_len > current_len) {
			current_len = required_len + 1;
			output = unique_ptr<char[]>{new char[current_len]};
		}
		//}
		assert(strlen(input_data[i]) < current_len);
		REVERSE_FUNCTION(input_data[i], output.get());

		result_data[i] = result.AddString(output.get());
	});
}

static void reverse_chunk_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	reverse_function<strreverse>(args.data[0], result);
}

void ReverseFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("reverse", {SQLType::VARCHAR}, SQLType::VARCHAR, reverse_chunk_function));
}

} // namespace duckdb
