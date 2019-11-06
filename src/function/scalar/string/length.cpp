#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void length_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                            BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	auto &input = inputs[0];
	assert(input.type == TypeId::VARCHAR);

	result.Initialize(TypeId::BIGINT);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;

	auto result_data = (int64_t *)result.data;
	auto input_data = (const char **)input.data;
	VectorOperations::Exec(input, [&](index_t i, index_t k) {
		if (input.nullmask[i]) {
			return;
		}
		int64_t length = 0;
		for (index_t str_idx = 0; input_data[i][str_idx]; str_idx++) {
			length += (input_data[i][str_idx] & 0xC0) != 0x80;
		}
		result_data[i] = length;
	});
}

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	// TODO: extend to support arbitrary number of arguments, not only two
	set.AddFunction(ScalarFunction("length", {SQLType::VARCHAR}, SQLType::BIGINT, length_function));
}

} // namespace duckdb
