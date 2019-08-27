#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void ln_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Ln(inputs[0], result);
}

void Ln::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("ln", { SQLType::DOUBLE }, SQLType::DOUBLE, ln_function));
}

}
