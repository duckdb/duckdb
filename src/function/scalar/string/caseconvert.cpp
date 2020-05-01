#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

template <bool IS_UPPER> static string_t strcase_unicode(Vector &result, const char *input_data, idx_t input_length) {
	// first figure out the output length
	// optimization: if only ascii then input_length = output_length
	idx_t output_length = 0;
	for (idx_t i = 0; i < input_length;) {
		if (input_data[i] & 0x80) {
			// unicode
			int sz = 0;
			int codepoint = utf8proc_codepoint(input_data + i, sz);
			int converted_codepoint = IS_UPPER ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
			int new_sz = utf8proc_codepoint_length(converted_codepoint);
			assert(new_sz >= 0);
			output_length += new_sz;
			i += sz;
		} else {
			// ascii
			output_length++;
			i++;
		}
	}
	auto result_str = StringVector::EmptyString(result, output_length);
	auto result_data = result_str.GetData();

	for (idx_t i = 0; i < input_length;) {
		if (input_data[i] & 0x80) {
			// non-ascii character
			int sz = 0, new_sz = 0;
			int codepoint = utf8proc_codepoint(input_data + i, sz);
			int converted_codepoint = IS_UPPER ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
			const auto success = utf8proc_codepoint_to_utf8(converted_codepoint, new_sz, result_data);
			assert(success);
			result_data += new_sz;
			i += sz;
		} else {
			// ascii
			*result_data = IS_UPPER ? toupper(input_data[i]) : tolower(input_data[i]);
			result_data++;
			i++;
		}
	}
	result_str.Finalize();
	return result_str;
}

template <bool IS_UPPER> static void caseconvert_function(Vector &input, Vector &result, idx_t count) {
	assert(input.type == TypeId::VARCHAR);

	UnaryExecutor::Execute<string_t, string_t, true>(input, result, count, [&](string_t input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		return strcase_unicode<IS_UPPER>(result, input_data, input_length);
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

ScalarFunction LowerFun::GetFunction() {
	return ScalarFunction({SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_lower_function);
}

void LowerFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"lower", "lcase"}, LowerFun::GetFunction());
}

void UpperFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"upper", "ucase"},
	                ScalarFunction({SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_upper_function));
}

} // namespace duckdb
