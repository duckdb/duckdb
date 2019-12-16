#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
static void add_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                         Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Add(inputs[0], inputs[1], result);
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
		functions.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// - [subtract]
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
static void multiply_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                              BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Multiply(inputs[0], inputs[1], result);
}

void MultiplyFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("*");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, multiply_function));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// / [divide]
//===--------------------------------------------------------------------===//
static void divide_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                            BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Divide(inputs[0], inputs[1], result);
}

void DivideFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("/");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, divide_function));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// % [modulo]
//===--------------------------------------------------------------------===//
static void mod_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                         Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Modulo(inputs[0], inputs[1], result);
}

void ModFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("%");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, mod_function));
	}
	set.AddFunction(functions);
	functions.name = "mod";
	set.AddFunction(functions);
}

} // namespace duckdb
