#include "duckdb/function/scalar/trigonometric_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

static void cos_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                         Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Cos(inputs[0], result);
}

void CosFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("cos", {SQLType::DOUBLE}, SQLType::DOUBLE, cos_function));
}

} // namespace duckdb
