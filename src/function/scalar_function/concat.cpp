#include "function/scalar_function/concat.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

void concat_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result) {
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

	// bool has_stats = expr.function->children[0]->stats.has_stats && expr.function->children[1]->stats.has_stats;
	index_t current_len = 0;
	unique_ptr<char[]> output;

	VectorOperations::BinaryExec(input1, input2, result,
	                             [&](index_t input1_index, index_t input2_index, index_t result_index) {
		                             if (result.nullmask[result_index]) {
			                             return;
		                             }
		                             auto input1 = input1_data[input1_index];
		                             auto input2 = input2_data[input2_index];
		                             index_t len1 = strlen(input1), len2 = strlen(input2);
		                             index_t required_len = len1 + len2 + 1;
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
bool concat_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 2 && arguments[0].id == SQLTypeId::VARCHAR && arguments[1].id == SQLTypeId::VARCHAR;
}

SQLType concat_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::VARCHAR);
}

} // namespace duckdb
