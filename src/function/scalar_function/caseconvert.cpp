#include "function/scalar_function/caseconvert.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

typedef void (*str_function)(const char *input, char *output);

// TODO: this does not handle UTF characters yet.
static void strtoupper(const char *input, char *output) {
	while (*input) {
		*output = toupper((unsigned char)*input);
		input++;
		output++;
	}
	*output = '\0';
}

static void strtolower(const char *input, char *output) {
	while (*input) {
		*output = tolower((unsigned char)*input);
		input++;
		output++;
	}
	*output = '\0';
}

template <str_function CASE_FUNCTION>
static void caseconvert_function(Vector inputs[], BoundFunctionExpression &expr, Vector &result) {
	auto &input = inputs[0];
	assert(input.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;

	auto result_data = (const char **)result.data;
	auto input_data = (const char **)input.data;

	// bool has_stats = expr.function->children[0]->stats.has_stats;
	index_t current_len = 0;
	unique_ptr<char[]> output;
	// if (has_stats) {
	// 	// stats available, pre-allocate the result chunk
	// 	current_len = expr.function->children[0]->stats.maximum_string_length + 1;
	// 	output = unique_ptr<char[]>{new char[current_len]};
	// }

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
		CASE_FUNCTION(input_data[i], output.get());

		result_data[i] = result.string_heap.AddString(output.get());
	});
}

void caseconvert_upper_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	caseconvert_function<strtoupper>(inputs, expr, result);
}

void caseconvert_lower_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	caseconvert_function<strtolower>(inputs, expr, result);
}

bool caseconvert_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 1 && arguments[0].id == SQLTypeId::VARCHAR;
}

SQLType caseconvert_get_return_type(vector<SQLType> &arguments) {
	return arguments[0];
}

} // namespace duckdb
