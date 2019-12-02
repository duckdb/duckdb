#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void round_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                           BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Round(inputs[0], inputs[1], result);
}

void RoundFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet round("round");
	for (auto &type : SQLType::NUMERIC) {
		round.AddFunction(ScalarFunction({type, SQLType::INTEGER}, type, round_function));
	}
	set.AddFunction(round);
}

} // namespace duckdb
