
#include "function/scalar_function/substring.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {

void substring_function(Vector inputs[], size_t input_count, Vector &result) {
	assert(input_count == 3);
	auto &input = inputs[0];
	auto &offset = inputs[1];
	auto &length = inputs[2];
	assert(input.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input.nullmask;

	auto result_data = (const char **)result.data;
	auto input_data = (const char **)input.data;
	auto offset_data = (int *)offset.data;
	auto length_data = (int *)length.data;

	VectorOperations::TernaryExec(
	    input, offset, length, result,
	    [&](size_t input_index, size_t offset_index, size_t length_index,
	        size_t result_index) {
		    auto input = input_data[input_index];
		    auto offset = offset_data[offset_index] - 1;
		    auto length = length_data[length_index];

		    // create the output string
		    char output[length + 1];
		    int input_offset = 0;
		    while (input[input_offset] && input_offset < offset) {
			    input_offset++;
		    }
		    if (!input[input_offset]) {
			    // out of range, return empty string
			    output[0] = '\0';
		    } else {
			    // now limit the string
			    size_t write_offset = 0;
			    while (input[input_offset + write_offset] &&
			           write_offset < length) {
				    output[write_offset] = input[input_offset + write_offset];
				    write_offset++;
			    }
			    output[write_offset] = '\0';
		    }

		    result_data[result_index] = result.string_heap.AddString(output);
	    });
}

bool substring_matches_arguments(std::vector<TypeId> &arguments) {
	return arguments.size() == 3 && arguments[0] == TypeId::VARCHAR &&
	       arguments[1] == TypeId::INTEGER && arguments[2] == TypeId::INTEGER;
}

TypeId substring_get_return_type(std::vector<TypeId> &arguments) {
	return TypeId::VARCHAR;
}

} // namespace function
} // namespace duckdb
