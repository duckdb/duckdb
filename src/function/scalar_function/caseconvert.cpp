#include "function/scalar_function/caseconvert.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

enum CaseconvertDirection { UPPER, LOWER };

// TODO: this does not handle UTF characters yet.

static void strtoupper(char *str) {
	while (*str) {
		*(str) = toupper((unsigned char)*str);
		str++;
	}
}

static void strtolower(char *str) {
	while (*str) {
		*(str) = tolower((unsigned char)*str);
		str++;
	}
}

static void caseconvert_function(Vector inputs[], CaseconvertDirection direction, size_t max_str_len, Vector &result) {
	auto &input = inputs[0];
	assert(input.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;

	auto result_data = (const char **)result.data;
	auto input_data = (const char **)input.data;
	auto output_uptr = unique_ptr<char[]>{new char[max_str_len + 1]};
	char *output = output_uptr.get();

	VectorOperations::Exec(input, [&](size_t i, size_t k) {
		if (input.nullmask[i]) {
			return;
		}
		assert(strlen(input_data[i]) <= max_str_len);

		strncpy(output, input_data[i], strlen(input_data[i]));
		output[strlen(input_data[i])] = '\0';

		switch (direction) {
		case CaseconvertDirection::UPPER:
			strtoupper(output);
			break;
		case CaseconvertDirection::LOWER:
			strtolower(output);
			break;
		default:
			throw Exception("Unknown direction");
		}

		result_data[i] = result.string_heap.AddString(output);
	});
}

void caseconvert_upper_function(Vector inputs[], size_t input_count, Expression &expr, Vector &result) {
	assert(input_count == 1);
	caseconvert_function(inputs, CaseconvertDirection::UPPER, expr.children[0]->stats.maximum_string_length, result);
}

void caseconvert_lower_function(Vector inputs[], size_t input_count, Expression &expr, Vector &result) {
	assert(input_count == 1);
	caseconvert_function(inputs, CaseconvertDirection::LOWER, expr.children[0]->stats.maximum_string_length, result);
}

bool caseconvert_matches_arguments(vector<TypeId> &arguments) {
	return arguments.size() == 1 && arguments[0] == TypeId::VARCHAR;
}

TypeId caseconvert_get_return_type(vector<TypeId> &arguments) {
	return TypeId::VARCHAR;
}

} // namespace function
} // namespace duckdb
