#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void add_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                         Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Add(inputs[0], inputs[1], result);
}

static void unary_add_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                               BoundFunctionExpression &expr, Vector &result) {
	inputs[0].Move(result);
}

void AddFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("+");
	// binary add function adds two numbers together
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, add_function));
	}
	// we can add integers to dates
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTEGER}, SQLType::DATE, add_function));
	functions.AddFunction(ScalarFunction({SQLType::INTEGER, SQLType::DATE}, SQLType::DATE, add_function));
	// unary add function is a nop, but only exists for numeric types
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type}, type, unary_add_function));
	}
	set.AddFunction(functions);
}
