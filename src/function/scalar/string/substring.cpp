#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

using namespace std;

namespace duckdb {

static string_t substring_scalar_function(string_t input, int offset, int length, unique_ptr<char[]> &output,
                                          idx_t &current_len) {
	// reduce offset by one because SQL starts counting at 1
	offset--;

	if (offset < 0 || length < 0) {
		throw Exception("SUBSTRING cannot handle negative offsets");
	}

	idx_t required_len = input.GetSize() + 1;
	if (required_len > current_len) {
		// need a resize
		current_len = required_len;
		output = unique_ptr<char[]>{new char[required_len]};
	}

	// UTF8 chars can use more than one byte
	idx_t input_char_offset = 0;
	idx_t input_byte_offset = 0;
	idx_t output_byte_offset = 0;

	auto input_string = input.GetData();

	while (input_string[input_byte_offset]) {
		char b = input_string[input_byte_offset++];
		input_char_offset += (b & 0xC0) != 0x80;
		if (input_char_offset > (idx_t)(offset + length)) {
			break;
		}
		if (input_char_offset > (idx_t)offset) {
			output[output_byte_offset++] = b;
		}
	}
	// terminate output
	output[output_byte_offset] = '\0';
	return string_t(output.get(), output_byte_offset);
}

static void substring_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 3 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT32 &&
	       args.data[2].type == TypeId::INT32);
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];
	auto &length_vector = args.data[2];

	idx_t current_len = 0;
	unique_ptr<char[]> output;
	TernaryExecutor::Execute<string_t, int, int, string_t, true>(
	    input_vector, offset_vector, length_vector, result, [&](string_t input_string, int offset, int length) {
		    return result.AddString(substring_scalar_function(input_string, offset, length, output, current_len));
	    });
}

void SubstringFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("substring",       // name of function
	                               {SQLType::VARCHAR, // argument list
	                                SQLType::INTEGER, SQLType::INTEGER},
	                               SQLType::VARCHAR,     // return type
	                               substring_function)); // pointer to function implementation
}

} // namespace duckdb
