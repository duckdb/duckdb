#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

static void concat_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                            BoundFunctionExpression &expr, Vector &result) {
	assert(input_count >= 2 && input_count <= 64);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = 0;
	for (index_t i = 0; i < input_count; i++) {
		auto &input = inputs[i];
		assert(input.type == TypeId::VARCHAR);
		if (!input.IsConstant()) {
			result.sel_vector = input.sel_vector;
			result.count = input.count;
		}
		// Any null input makes concat's result null.
		result.nullmask |= input.nullmask;
	}

	// bool has_stats = expr.function->children[0]->stats.has_stats && expr.function->children[1]->stats.has_stats;
	auto result_data = (const char **)result.data;
	index_t current_len = 0;
	unique_ptr<char[]> output;

	VectorOperations::MultiaryExec(inputs, input_count, result, [&](vector<index_t> mul, index_t result_index) {
		if (result.nullmask[result_index]) {
			return;
		}

		// calculate length of result string
		vector<const char *> input_chars(input_count);
		index_t required_len = 0;
		for (index_t i = 0; i < input_count; i++) {
			int current_index = mul[i] * result_index;
			input_chars[i] = ((const char **)inputs[i].data)[current_index];
			required_len += strlen(input_chars[i]);
		}
		required_len++;

		// allocate length of result string
		if (required_len > current_len) {
			current_len = required_len;
			output = unique_ptr<char[]>{new char[required_len]};
		}

		// actual concatenation
		int length_so_far = 0;
		for (index_t i = 0; i < input_count; i++) {
			int len = strlen(input_chars[i]);
			memcpy(output.get() + length_so_far, input_chars[i], sizeof(char) * len);
			length_so_far += len;
		}
		output[length_so_far] = '\0';
		result_data[result_index] = result.string_heap.AddString(output.get());
	});
}

void ConcatFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction concat =
	    ScalarFunction("concat", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR, concat_function);
	concat.varargs = SQLType::VARCHAR;
	set.AddFunction(concat);
	concat.name = "||";
	set.AddFunction(concat);
}

} // namespace duckdb
