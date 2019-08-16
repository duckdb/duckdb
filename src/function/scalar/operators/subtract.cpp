#include "function/scalar/operators.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void subtract_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Subtract(inputs[0], inputs[1], result);
}

void Subtract::RegisterFunction(BuiltinFunctions &set) {
	FunctionSet functions("-");
	for(auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({ type, type }, type, subtract_function));
	}
	functions.AddFunction(ScalarFunction({ SQLType::DATE, SQLType::DATE }, SQLType::INTEGER, subtract_function));
	functions.AddFunction(ScalarFunction({ SQLType::DATE, SQLType::INTEGER }, SQLType::DATE, subtract_function));
	set.AddFunction(functions);
}
