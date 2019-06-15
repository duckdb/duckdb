#include "function/scalar_function/substring.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void substring_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                        Vector &result) {
	assert(input_count == 3);
	auto &input = inputs[0];
	auto &offset = inputs[1];
	auto &length = inputs[2];

	assert(input.type == TypeId::VARCHAR);
	assert(offset.type == TypeId::INTEGER);
	assert(length.type == TypeId::INTEGER);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask = input.nullmask;

	auto result_data = (const char **)result.data;
	auto input_data = (const char **)input.data;
	auto offset_data = (int *)offset.data;
	auto length_data = (int *)length.data;

	// bool has_stats = expr.function->children[0]->stats.has_stats;
	index_t current_len = 0;
	unique_ptr<char[]> output;
	// if (has_stats) {
	// 	// stats available, pre-allocate the result chunk
	// 	current_len = expr.function->children[0]->stats.maximum_string_length + 1;
	// 	output = unique_ptr<char[]>{new char[current_len]};
	// }

	VectorOperations::TernaryExec(
	    input, offset, length, result,
	    [&](index_t input_index, index_t offset_index, index_t length_index, index_t result_index) {
		    auto input_string = input_data[input_index];
		    auto offset = offset_data[offset_index] - 1; // minus one because SQL starts counting at 1
		    auto length = length_data[length_index];

		    if (input.nullmask[input_index]) {
			    return;
		    }

		    if (offset < 0 || length < 0) {
			    throw Exception("SUBSTRING cannot handle negative offsets");
		    }

		    index_t required_len = strlen(input_string) + 1;
		    if (required_len > current_len) {
			    current_len = required_len;
			    output = unique_ptr<char[]>{new char[required_len]};
		    }

		    // UTF8 chars can use more than one byte
		    index_t input_char_offset = 0;
		    index_t input_byte_offset = 0;
		    index_t output_byte_offset = 0;

		    while (input_string[input_byte_offset]) {
			    char b = input_string[input_byte_offset++];
			    input_char_offset += (b & 0xC0) != 0x80;
			    if (input_char_offset > (index_t)(offset + length)) {
				    break;
			    }
			    if (input_char_offset > (index_t)offset) {
				    output[output_byte_offset++] = b;
			    }
		    }
		    // terminate output
		    output[output_byte_offset] = '\0';
		    result_data[result_index] = result.string_heap.AddString(output.get());
	    });
}

bool substring_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 3 && arguments[0].id == SQLTypeId::VARCHAR && arguments[1].id == SQLTypeId::INTEGER &&
	       arguments[2].id == SQLTypeId::INTEGER;
}

SQLType substring_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::VARCHAR);
}

} // namespace duckdb
