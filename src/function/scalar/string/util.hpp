#pragma once

#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

#include "utf8proc.hpp"

namespace duckdb {

static string_t substring_ascii_only(Vector &result, const char *input_data, int offset, int length) {
	auto result_string = StringVector::EmptyString(result, length);
	auto result_data = result_string.GetData();
	memcpy(result_data, input_data + offset, length);
	result_string.Finalize();
	return result_string;
}


static string_t substring_scalar_function(Vector &result, string_t input, int offset, int length,
										  unique_ptr<char[]> &output, idx_t &current_len) {
	// reduce offset by one because SQL starts counting at 1
	offset--;
	if (offset < 0 || length < 0) {
		throw Exception("SUBSTRING cannot handle negative offsets");
	}
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	// check if there is any non-ascii
	bool ascii_only = true;
	int ascii_end = std::min(offset + length + 1, (int)input_size);
	for (int i = 0; i < ascii_end; i++) {
		if (input_data[i] & 0x80) {
			ascii_only = false;
			break;
		}
	}

	if (length == 0) {
		auto result_string = StringVector::EmptyString(result, 0);
		result_string.Finalize();
		return result_string;
	}

	if (ascii_only) {
		// ascii only
		length = std::min(offset + length, (int)input_size);
		if (offset >= length) {
			return string_t((uint32_t)0);
		}
		return substring_ascii_only(result, input_data, offset, length - offset);
	}

	// size is at most the input size: alloc it
	idx_t required_len = input_size + 1;
	if (required_len > current_len) {
		// need a resize
		current_len = required_len;
		output = unique_ptr<char[]>{new char[required_len]};
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

// length returns the size in characters
struct StringLengthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		for (idx_t i = 0; i < input_length; i++) {
			if (input_data[i] & 0x80) {
				int64_t length = 0;
				// non-ascii character: use grapheme iterator on remainder of string
				utf8proc_grapheme_callback(input_data, input_length, [&](size_t start, size_t end) {
					length++;
					return true;
				});
				return length;
			}
		}
		return input_length;
	}
};

}
