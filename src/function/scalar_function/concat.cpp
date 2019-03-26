#include "function/scalar_function/concat.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {
namespace function {

void concat_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 2);
	auto &input1 = inputs[0];
	auto &input2 = inputs[1];
	assert(input1.type == TypeId::VARCHAR);
	assert(input2.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input1.nullmask | input2.nullmask;

	auto result_data = (const char **)result.data;
	auto input1_data = (const char **)input1.data;
	auto input2_data = (const char **)input2.data;

	bool has_stats = expr.function->children[0]->stats.has_stats && expr.function->children[1]->stats.has_stats;
	size_t current_len = 0;
	unique_ptr<char[]> output;
	if (has_stats) {
		// stats available, pre-allocate the result chunk
		current_len = expr.function->children[0]->stats.maximum_string_length +
		              expr.function->children[1]->stats.maximum_string_length + 1;
		output = unique_ptr<char[]>{new char[current_len]};
	}

	VectorOperations::BinaryExec(input1, input2, result,
	                             [&](size_t input1_index, size_t input2_index, size_t result_index) {
		                             if (result.nullmask[result_index]) {
			                             return;
		                             }
		                             auto input1 = input1_data[input1_index];
		                             auto input2 = input2_data[input2_index];
		                             size_t len1 = strlen(input1), len2 = strlen(input2);
		                             size_t required_len = len1 + len2 + 1;
		                             if (required_len > current_len) {
			                             current_len = required_len;
			                             output = unique_ptr<char[]>{new char[required_len]};
		                             }
		                             strncpy(output.get(), input1, len1);
		                             strncpy(output.get() + len1, input2, len2);
		                             output[len1 + len2] = '\0';
		                             result_data[result_index] = result.string_heap.AddString(output.get());
	                             });
}

// TODO: extend to support arbitrary number of arguments, not only two
bool concat_matches_arguments(vector<TypeId> &arguments) {
	return arguments.size() == 2 && arguments[0] == TypeId::VARCHAR && arguments[1] == TypeId::VARCHAR;
}

TypeId concat_get_return_type(vector<TypeId> &arguments) {
	return TypeId::VARCHAR;
}

} // namespace function
} // namespace duckdb
