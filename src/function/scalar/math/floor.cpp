#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void floor_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                           BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Floor(inputs[0], result);
}

void FloorFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet floor("floor");
	for (auto &type : SQLType::NUMERIC) {
		floor.AddFunction(ScalarFunction({type}, type, floor_function));
	}
	set.AddFunction(floor);
}

} // namespace duckdb
