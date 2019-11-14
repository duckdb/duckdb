#include "duckdb/function/scalar/trigonometric_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

static void atan2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                           BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 2);

	inputs[0].Cast(TypeId::DOUBLE);
	inputs[1].Cast(TypeId::DOUBLE);

	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ATan2(inputs[0], inputs[1], result);
}

void Atan2Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("atan2", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE, atan2_function));
}

} // namespace duckdb
