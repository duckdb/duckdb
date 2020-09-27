#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

string_t SubstringFun::substring_ascii_only(Vector &result, const char *input_data, int offset, int length) {
	auto result_string = StringVector::EmptyString(result, length);
	auto result_data = result_string.GetData();
	memcpy(result_data, input_data + offset, length);
	result_string.Finalize();
	return result_string;
}

string_t SubstringFun::substring_scalar_function(Vector &result, string_t input, int offset, int length,
                                                 unique_ptr<char[]> &output, idx_t &current_len) {
	if (length < 0) {
		throw Exception("SUBSTRING cannot handle negative lengths");
	}

	if (length == 0) {
		auto result_string = StringVector::EmptyString(result, 0);
		result_string.Finalize();
		return result_string;
	}

	// reduce offset by one because SQL starts counting at 1
	// special case: if offset is zero, reduce length by 1 to avoid copying uninitialised memory
	if (offset > 0) {
		--offset;
	} else if (offset == 0) {
		--length;
	}
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	// check if there is any non-ascii
	bool ascii_only = true;
	int ascii_begin = 0;
	int ascii_end = (int)input_size;
	if (offset < 0) {
		ascii_begin = MaxValue<int>(input_size + offset, ascii_begin);
	} else {
		// scan one further to check for a grapheme cluster
		ascii_end = MinValue<int>(offset + length + 1, ascii_end);
	}
	for (auto i = ascii_begin; i < ascii_end; ++i) {
		if (input_data[i] & 0x80) {
			ascii_only = false;
			break;
		}
	}

	if (ascii_only) {
		if (offset < 0) {
			offset = ascii_begin;
		}
		length = MinValue<int>(offset + length, input_size);
		if (offset >= length) {
			return string_t((uint32_t)0);
		}
		return SubstringFun::substring_ascii_only(result, input_data, offset, length - offset);
	}

	// size is at most the input size: alloc it
	idx_t required_len = input_size + 1;
	if (required_len > current_len) {
		// need a resize
		current_len = required_len;
		output = unique_ptr<char[]>{new char[required_len]};
	}

	// if we are slicing Unicode, get the positive offset by computing the length
	if (offset < 0) {
		utf8proc_grapheme_callback(input_data, input_size, [&](size_t start, size_t end) {
			++offset;
			return true;
		});
		offset = MaxValue<int>(offset, 0);
	}

	// use grapheme iterator to iterate over the characters
	int current_offset = 0;
	int output_size = 0;
	utf8proc_grapheme_callback(input_data, input_size, [&](size_t start, size_t end) {
		if (current_offset >= offset) {
			// this character belongs to the output: copy it there
			memcpy(output.get() + output_size, input_data + start, end - start);
			output_size += end - start;
		}
		current_offset++;
		// stop iterating after we have exceeded the required characters
		return current_offset < offset + length;
	});
	output[output_size] = '\0';
	return StringVector::AddString(result, output.get(), output_size);
}

static void substring_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];
	auto &offset_vector = args.data[1];
	auto &length_vector = args.data[2];

	idx_t current_len = 0;
	unique_ptr<char[]> output;
	TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
	    input_vector, offset_vector, length_vector, result, args.size(),
	    [&](string_t input_string, int32_t offset, int32_t length) {
		    return SubstringFun::substring_scalar_function(result, input_string, offset, length, output, current_len);
	    });
}

void SubstringFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"substring", "substr"},
	                ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER},
	                               LogicalType::VARCHAR, substring_function));
}

} // namespace duckdb
