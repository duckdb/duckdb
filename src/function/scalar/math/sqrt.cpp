#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void sqrt_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Sqrt(inputs[0], result);
}

void Sqrt::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("sqrt", { SQLType::DOUBLE }, SQLType::DOUBLE, sqrt_function));
}

}
