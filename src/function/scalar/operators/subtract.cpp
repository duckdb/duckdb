#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

static void subtract_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                              BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Subtract(inputs[0], inputs[1], result);
}

static void unary_subtract_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                    BoundFunctionExpression &expr, Vector &result) {
	Value minus_one = Value::Numeric(inputs[0].type, -1);
	Vector right;
	right.Reference(minus_one);

	result.Initialize(inputs[0].type);
	VectorOperations::Multiply(inputs[0], right, result);
}

void SubtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("-");
	// binary subtract function "a - b", subtracts b from a
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, subtract_function));
	}
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::DATE}, SQLType::INTEGER, subtract_function));
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTEGER}, SQLType::DATE, subtract_function));
	// unary subtract function, negates the input (i.e. multiplies by -1)
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type}, type, unary_subtract_function));
	}
	set.AddFunction(functions);
}
