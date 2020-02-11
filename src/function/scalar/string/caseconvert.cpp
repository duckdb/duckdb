#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

typedef void (*str_function)(const char *input, char *output);

// TODO: this does not handle UTF characters yet.
static void strtoupper(const char *input, char *output) {
	while (*input) {
		*output = toupper((unsigned char)*input);
		input++;
		output++;
	}
	*output = '\0';
}

static void strtolower(const char *input, char *output) {
	while (*input) {
		*output = tolower((unsigned char)*input);
		input++;
		output++;
	}
	*output = '\0';
}

template <str_function CASE_FUNCTION> static void caseconvert_function(Vector &input, Vector &result) {
	assert(input.type == TypeId::VARCHAR);

	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;

	auto result_data = (const char **)result.GetData();
	auto input_data = (const char **)input.GetData();

	index_t current_len = 0;
	unique_ptr<char[]> output;
	VectorOperations::Exec(input, [&](index_t i, index_t k) {
		if (input.nullmask[i]) {
			return;
		}
		// if (!has_stats) {
		// no stats available, might need to reallocate
		index_t required_len = strlen(input_data[i]) + 1;
		if (required_len > current_len) {
			current_len = required_len + 1;
			output = unique_ptr<char[]>{new char[current_len]};
		}
		//}
		assert(strlen(input_data[i]) < current_len);
		CASE_FUNCTION(input_data[i], output.get());

		result_data[i] = result.AddString(output.get());
	});
}

static void caseconvert_upper_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count == 1);
	caseconvert_function<strtoupper>(args.data[0], result);
}

static void caseconvert_lower_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count == 1);
	caseconvert_function<strtolower>(args.data[0], result);
}

void LowerFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("lower", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_lower_function));
}

void UpperFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("upper", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_upper_function));
}

} // namespace duckdb
