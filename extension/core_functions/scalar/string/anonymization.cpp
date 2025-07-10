#include "core_functions/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

// Helper function to count Unicode characters (not bytes)
static idx_t GetCharacterCount(const char *data, idx_t size) {
	idx_t char_count = 0;
	for (idx_t i = 0; i < size; i++) {
		// Count UTF-8 start bytes (not continuation bytes)
		if ((data[i] & 0xc0) != 0x80) {
			char_count++;
		}
	}
	return char_count;
}

// Basic anonymize function that replaces all characters with 'x'
static void AnonymizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	
	UnaryExecutor::Execute<string_t, string_t>(
		input, result, args.size(),
		[&](string_t input) {
			auto input_data = input.GetData();
			auto input_size = input.GetSize();
			
			// Count Unicode characters, not bytes
			idx_t char_count = GetCharacterCount(input_data, input_size);
			
			// Create output string with 'x' repeated char_count times
			string output_str(char_count, 'x');
			return StringVector::AddString(result, output_str);
		});
}

// Advanced anonymize function with customizable replacement character
static void AnonymizeAdvancedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &replacement_char = args.data[1];
	
	BinaryExecutor::Execute<string_t, string_t, string_t>(
		input, replacement_char, result, args.size(),
		[&](string_t input, string_t replacement) {
			auto input_data = input.GetData();
			auto input_size = input.GetSize();
			auto repl_data = replacement.GetData();
			auto repl_size = replacement.GetSize();
			
			// Handle empty replacement string
			if (repl_size == 0) {
				idx_t char_count = GetCharacterCount(input_data, input_size);
				string output_str(char_count, 'x');
				return StringVector::AddString(result, output_str);
			}
			
			// Count Unicode characters in input
			idx_t char_count = GetCharacterCount(input_data, input_size);
			
			// Get first character from replacement string (could be multi-byte)
			string repl_char_str;
			if (repl_data[0] & 0x80) {
				// Multi-byte UTF-8 character
				utf8proc_int32_t codepoint;
				auto bytes = utf8proc_iterate(reinterpret_cast<const utf8proc_uint8_t*>(repl_data), repl_size, &codepoint);
				if (bytes > 0) {
					repl_char_str = string(repl_data, bytes);
				} else {
					repl_char_str = "x";
				}
			} else {
				// ASCII character
				repl_char_str = string(1, repl_data[0]);
			}
			
			// Create output string by repeating the replacement character
			string output_str;
			output_str.reserve(char_count * repl_char_str.size());
			for (idx_t i = 0; i < char_count; i++) {
				output_str += repl_char_str;
			}
			
			return StringVector::AddString(result, output_str);
		});
}

// Email anonymization function that preserves domain structure
static void AnonymizeEmailFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	
	UnaryExecutor::Execute<string_t, string_t>(
		input, result, args.size(),
		[&](string_t input) {
			auto input_data = input.GetData();
			auto input_size = input.GetSize();
			string input_str(input_data, input_size);
			
			// Find the @ symbol
			size_t at_pos = input_str.find('@');
			if (at_pos == string::npos) {
				// No @ symbol, treat as regular string
				idx_t char_count = GetCharacterCount(input_data, input_size);
				string output_str(char_count, 'x');
				return StringVector::AddString(result, output_str);
			}
			
			// Extract local part (before @) and domain part (after @)
			string local_part = input_str.substr(0, at_pos);
			string domain_part = input_str.substr(at_pos + 1);
			
			// Count characters in local part (Unicode-aware)
			idx_t local_char_count = GetCharacterCount(local_part.c_str(), local_part.size());
			
			// Anonymize local part
			string anonymized_local(local_char_count, 'x');
			
			// Keep domain part unchanged
			string result_str = anonymized_local + "@" + domain_part;
			return StringVector::AddString(result, result_str);
		});
}

// Helper function to get character positions in UTF-8 string
static vector<idx_t> GetCharacterPositions(const char *data, idx_t size) {
	vector<idx_t> positions;
	positions.push_back(0); // Start position
	
	for (idx_t i = 0; i < size; i++) {
		// Check for UTF-8 start byte (not continuation byte)
		if ((data[i] & 0xc0) != 0x80) {
			if (i > 0) {
				positions.push_back(i);
			}
		}
	}
	positions.push_back(size); // End position
	return positions;
}

// Partial anonymization function that preserves start_chars and end_chars
static void AnonymizePartialFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &start_chars = args.data[1];
	auto &end_chars = args.data[2];
	
	TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
		input, start_chars, end_chars, result, args.size(),
		[&](string_t input, int32_t start_preserve, int32_t end_preserve) {
			auto input_data = input.GetData();
			auto input_size = input.GetSize();
			
			// Handle edge cases
			if (start_preserve < 0) start_preserve = 0;
			if (end_preserve < 0) end_preserve = 0;
			
			// Get character positions for Unicode-aware processing
			auto char_positions = GetCharacterPositions(input_data, input_size);
			idx_t total_chars = char_positions.size() - 1; // -1 because last position is end marker
			
			// Handle case where string is empty or very short
			if (total_chars == 0) {
				return StringVector::AddString(result, string(input_data, input_size));
			}
			
			// Adjust preserve counts if they exceed string length
			if ((idx_t)start_preserve >= total_chars) {
				// Start preserve covers entire string
				return StringVector::AddString(result, string(input_data, input_size));
			}
			
			if ((idx_t)end_preserve >= total_chars) {
				// End preserve covers entire string
				return StringVector::AddString(result, string(input_data, input_size));
			}
			
			// If start + end >= total, don't mask anything
			if ((idx_t)(start_preserve + end_preserve) >= total_chars) {
				return StringVector::AddString(result, string(input_data, input_size));
			}
			
			// Calculate positions
			idx_t start_mask_pos = char_positions[start_preserve];
			idx_t end_mask_pos = char_positions[total_chars - end_preserve];
			idx_t mask_char_count = (total_chars - end_preserve) - start_preserve;
			
			// Build result string
			string result_str;
			
			// Add preserved start characters
			if (start_preserve > 0) {
				result_str += string(input_data, start_mask_pos);
			}
			
			// Add masked characters
			if (mask_char_count > 0) {
				result_str += string(mask_char_count, 'x');
			}
			
			// Add preserved end characters
			if (end_preserve > 0) {
				result_str += string(input_data + end_mask_pos, input_size - end_mask_pos);
			}
			
			return StringVector::AddString(result, result_str);
		});
}

ScalarFunction AnonymizeFun::GetFunction() {
	return ScalarFunction("anonymize", {LogicalType::VARCHAR}, LogicalType::VARCHAR, AnonymizeFunction);
}

ScalarFunctionSet AnonymizeAdvancedFun::GetFunctions() {
	ScalarFunctionSet set("anonymize_advanced");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, AnonymizeAdvancedFunction));
	return set;
}

ScalarFunction AnonymizeEmailFun::GetFunction() {
	return ScalarFunction("anonymize_email", {LogicalType::VARCHAR}, LogicalType::VARCHAR, AnonymizeEmailFunction);
}

ScalarFunctionSet AnonymizePartialFun::GetFunctions() {
	ScalarFunctionSet set("anonymize_partial");
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::VARCHAR, AnonymizePartialFunction));
	return set;
}

} // namespace duckdb