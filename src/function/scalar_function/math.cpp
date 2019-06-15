#include "function/scalar_function/math.hpp"
#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <cmath>

using namespace std;

namespace duckdb {

void abs_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	result.Initialize(inputs[0].type);
	VectorOperations::Abs(inputs[0], result);
}

void ceil_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Ceil(inputs[0], result);
}

void floor_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Floor(inputs[0], result);
}

void exp_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Exp(inputs[0], result);
}

void cbrt_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::CbRt(inputs[0], result);
}

void degrees_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Degrees(inputs[0], result);
}

void radians_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Radians(inputs[0], result);
}

void sqrt_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Sqrt(inputs[0], result);
}

void ln_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Ln(inputs[0], result);
}

void log10_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Log10(inputs[0], result);
}

void pi_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result) {
	assert(input_count == 0);
	result.Initialize(TypeId::DOUBLE);
	result.count = 1;
	VectorOperations::Set(result, Value(PI));
}

} // namespace duckdb
