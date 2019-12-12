#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

void concat_ws_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result) {
	assert(input_count >= 2 && input_count <= 64);

	// The first input parameter is the seperator
	auto &input_separator = inputs[0];
	auto input_separator_data = (const char **)input_separator.data;

	result.Initialize(TypeId::VARCHAR);

	// concat_ws becomes null only when the separator is null.
	result.nullmask = input_separator.nullmask;
	for (index_t i = 1; i < input_count; i++) {
		auto &input = inputs[i];
		assert(input.type == TypeId::VARCHAR);
		if (!input.IsConstant()) {
			result.sel_vector = input.sel_vector;
			result.count = input.count;
		}
	}

	// bool has_stats = expr.function->children[0]->stats.has_stats && expr.function->children[1]->stats.has_stats;
	auto result_data = (const char **)result.data;
	index_t current_len = 0;
	unique_ptr<char[]> output;

	VectorOperations::MultiaryExec(inputs, input_count, result, [&](vector<index_t> mul, index_t result_index) {
		if (result.nullmask[result_index]) {
			return;
		}

		// calculate length of separator string
		auto separator = input_separator_data[mul[0] * result_index];
		index_t separator_length = strlen(separator);

		// calculate length of result string using the rest of the input parameters
		vector<const char *> input_chars(input_count);
		index_t required_len = 0;
		for (index_t i = 1; i < input_count; i++) {
			auto &input = inputs[i];
			int current_index = mul[i] * result_index;

			// Add the first non-separator string to the result string
			if (!input.nullmask[current_index]) {
				input_chars[i] = ((const char **)input.data)[current_index];
				// append the separator only when there is something preceding it
				if (i > 1 && required_len > 0)
					required_len += separator_length;
				required_len += strlen(input_chars[i]);
			}
		}
		required_len++;

		// allocate length of result string
		if (required_len > current_len) {
			current_len = required_len;
			output = unique_ptr<char[]>{new char[required_len]};
		}

		int length_so_far = 0;
		for (index_t i = 1; i < input_count; i++) {
			auto &input = inputs[i];
			int current_index = mul[i] * result_index;

			// Add the first non-separator string to the result string
			if (!input.nullmask[current_index]) {
				// append the separator only when there is something preceding it
				if (i > 1 && length_so_far > 0) {
					strncpy(output.get() + length_so_far, separator, separator_length);
					length_so_far += separator_length;
				}

				// append the next input string
				int input_length = strlen(input_chars[i]);
				memcpy(output.get() + length_so_far, input_chars[i], sizeof(char) * input_length);
				length_so_far += input_length;
			}
		}
		output[length_so_far] = '\0';
		result_data[result_index] = result.string_heap.AddString(output.get());
	});
}

void ConcatWSFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction concat_ws =
	    ScalarFunction("concat_ws", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR, concat_ws_function);
	concat_ws.varargs = SQLType::VARCHAR;
	set.AddFunction(concat_ws);
}

} // namespace duckdb
