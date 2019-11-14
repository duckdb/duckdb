#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void radians_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                             BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Radians(inputs[0], result);
}

void RadiansFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("radians", {SQLType::DOUBLE}, SQLType::DOUBLE, radians_function));
}

} // namespace duckdb
