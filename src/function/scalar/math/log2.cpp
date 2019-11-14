#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void log2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                          Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Log2(inputs[0], result);
}

void Log2Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("log2", {SQLType::DOUBLE}, SQLType::DOUBLE, log2_function));
}

} // namespace duckdb
