
#include "function/scalar_function/concat.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {
namespace function {

void concat_function(Vector inputs[], size_t input_count, Vector &result) {
	assert(input_count == 2);
	auto &input1 = inputs[0];
	auto &input2 = inputs[1];
	assert(input1.type == TypeId::VARCHAR);
	assert(input2.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input1.nullmask;

	auto result_data = (const char **)result.data;
	auto input1_data = (const char **)input1.data;
	auto input2_data = (const char **)input2.data;

	VectorOperations::BinaryExec(
	    input1, input2, result,
	    [&](size_t input1_index, size_t input2_index, size_t result_index) {
		    auto input1 = input1_data[input1_index];
		    auto input2 = input2_data[input2_index];
		    char output[strlen(input1) + strlen(input2) + 1];
		    strncpy(output, input1, strlen(input1));
		    strncpy(output + strlen(input1), input2, strlen(input2));
		    output[strlen(input1) + strlen(input2)] = '\0';
		    result_data[result_index] = result.string_heap.AddString(output);
	    });
}

// TODO: extend to support arbitrary number of arguments, not only two
bool concat_matches_arguments(std::vector<TypeId> &arguments) {
	return arguments.size() == 2 && arguments[0] == TypeId::VARCHAR &&
	       arguments[1] == TypeId::VARCHAR;
}

TypeId concat_get_return_type(std::vector<TypeId> &arguments) {
	return TypeId::VARCHAR;
}

} // namespace function
} // namespace duckdb
