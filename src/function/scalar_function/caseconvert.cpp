#include "function/scalar_function/caseconvert.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

enum CaseconvertDirection { UPPER, LOWER };

// TODO: this does not handle UTF characters yet.s

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

static void caseconvert_function(Vector inputs[], size_t input_count,
                                 Vector &result,
                                 CaseconvertDirection direction) {
	assert(input_count == 1);
	auto &input = inputs[0];
	assert(input.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input.nullmask;
	result.count = input.count;

	auto result_data = (const char **)result.data;
	auto input_data = (const char **)input.data;

	VectorOperations::Exec(input, [&](size_t i, size_t k) {
		if (input.nullmask[i]) {
			return;
		}
		char output[strlen(input_data[i]) + 1];
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

void caseconvert_upper_function(Vector inputs[], size_t input_count,
                                Vector &result) {
	caseconvert_function(inputs, input_count, result,
	                     CaseconvertDirection::UPPER);
}

void caseconvert_lower_function(Vector inputs[], size_t input_count,
                                Vector &result) {
	caseconvert_function(inputs, input_count, result,
	                     CaseconvertDirection::LOWER);
}

bool caseconvert_matches_arguments(std::vector<TypeId> &arguments) {
	return arguments.size() == 1 && arguments[0] == TypeId::VARCHAR;
}

TypeId caseconvert_get_return_type(std::vector<TypeId> &arguments) {
	return TypeId::VARCHAR;
}

} // namespace function
} // namespace duckdb
