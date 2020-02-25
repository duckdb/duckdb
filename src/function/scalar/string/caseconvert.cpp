#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

// TODO: this does not handle UTF characters yet.
template <class OP> static void strcase(const char *input_data, idx_t input_length, char *output) {
	for (idx_t i = 0; i < input_length; i++) {
		output[i] = OP::Operation(input_data[i]);
	}
	output[input_length] = '\0';
}

template <class OP> static void caseconvert_function(Vector &input, Vector &result) {
	assert(input.type == TypeId::VARCHAR);

	UnaryExecutor::Execute<string_t, string_t, true>(input, result, [&](string_t input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();

		auto target = result.EmptyString(input_length);
		strcase<OP>(input_data, input_length, target.GetData());
		return target;
	});
}

struct StringToUpper {
	static char Operation(char input) {
		return toupper(input);
	}
};

struct StringToLower {
	static char Operation(char input) {
		return tolower(input);
	}
};

static void caseconvert_upper_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	caseconvert_function<StringToUpper>(args.data[0], result);
}

static void caseconvert_lower_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	caseconvert_function<StringToLower>(args.data[0], result);
}

void LowerFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("lower", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_lower_function));
}

void UpperFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("upper", {SQLType::VARCHAR}, SQLType::VARCHAR, caseconvert_upper_function));
}

} // namespace duckdb
