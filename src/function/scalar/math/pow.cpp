#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

static void pow_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                         Vector &result) {
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Pow(inputs[0], inputs[1], result);
}

void PowFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction power_function("pow", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE, pow_function);
	set.AddFunction(power_function);
	power_function.name = "power";
	set.AddFunction(power_function);
}

} // namespace duckdb
