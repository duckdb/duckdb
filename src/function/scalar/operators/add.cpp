#include "function/scalar/operators.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void add_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Add(inputs[0], inputs[1], result);
}

void Add::RegisterFunction(BuiltinFunctions &set) {
	FunctionSet functions("+");
	for(auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({ type, type }, type, add_function));
	}
	// we can add integers to dates
	functions.AddFunction(ScalarFunction({ SQLType::DATE, SQLType::INTEGER }, SQLType::DATE, add_function));
	functions.AddFunction(ScalarFunction({ SQLType::INTEGER, SQLType::DATE }, SQLType::DATE, add_function));
	set.AddFunction(functions);
}
