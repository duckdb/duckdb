#include "function/scalar/operators.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void divide_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Divide(inputs[0], inputs[1], result);
}

void Divide::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("/");
	for(auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({ type, type }, type, divide_function));
	}
	set.AddFunction(functions);
}
