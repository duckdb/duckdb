#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

template <bool IS_UPPER>
static string_t strcase_unicode(Vector &result, const char *input_data, idx_t input_length, unique_ptr<char[]> &output,
                                idx_t &current_len) {
	if (input_length + 1 > current_len) {
		output = unique_ptr<char[]>(new char[input_length + 1]);
		current_len = input_length + 1;
	}
	idx_t output_length = 0;
	for (idx_t i = 0; i < input_length;) {
		if (input_data[i] & 0x80) {
			// non-ascii character
			int sz = 0;
			int codepoint = utf8proc_codepoint(input_data + i, sz);
			int converted_codepoint = IS_UPPER ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
			while (!utf8proc_codepoint_to_utf8(converted_codepoint, sz, output.get() + output_length,
			                                   current_len - output_length)) {
				// need to resize output
				current_len *= 2;
				auto new_output = unique_ptr<char[]>(new char[current_len]);
				memcpy(new_output.get(), output.get(), output_length);
				output = move(new_output);
			}
			output_length += sz;
			i += sz;
			continue;
		}
		output[output_length++] = IS_UPPER ? toupper(input_data[i]) : tolower(input_data[i]);
		i++;
	}
	output[output_length] = '\0';
	return StringVector::AddString(result, output.get(), input_length);
}

template <bool IS_UPPER> static void caseconvert_function(Vector &input, Vector &result, idx_t count) {
	assert(input.type == TypeId::VARCHAR);

	unique_ptr<char[]> output;
	idx_t output_length = 0;
	UnaryExecutor::Execute<string_t, string_t, true>(input, result, count, [&](string_t input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		return strcase_unicode<IS_UPPER>(result, input_data, input_length, output, output_length);
	});
}

static void caseconvert_upper_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	caseconvert_function<true>(args.data[0], result, args.size());
}

static void caseconvert_lower_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	caseconvert_function<false>(args.data[0], result, args.size());
}

void LowerFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("lower", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_lower_function));
	set.AddFunction(ScalarFunction("lcase", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_lower_function));
}

void UpperFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("upper", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_upper_function));
	set.AddFunction(ScalarFunction("ucase", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_upper_function));
}

} // namespace duckdb
