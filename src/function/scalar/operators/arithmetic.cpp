#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
static void add_function(DataChunk &input, ExpressionState &state, Vector &result) {
	VectorOperations::Add(input.data[0], input.data[1], result);
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
static void subtract_function(DataChunk &input, ExpressionState &state, Vector &result) {
	VectorOperations::Subtract(input.data[0], input.data[1], result);
}

struct NegateOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return -input;
	}
};

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
		functions.AddFunction(
		    ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<NegateOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
static void multiply_function(DataChunk &input, ExpressionState &state, Vector &result) {
	VectorOperations::Multiply(input.data[0], input.data[1], result);
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
static void divide_function(DataChunk &input, ExpressionState &state, Vector &result) {
	VectorOperations::Divide(input.data[0], input.data[1], result);
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
static void mod_function(DataChunk &input, ExpressionState &state, Vector &result) {
	VectorOperations::Modulo(input.data[0], input.data[1], result);
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
