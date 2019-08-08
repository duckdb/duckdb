#include "function/scalar_function/math.hpp"
#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include <cmath>
#include <random>

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

void log2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Log2(inputs[0], result);
}

void pi_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result) {
	assert(input_count == 0);
	result.Initialize(TypeId::DOUBLE);
	result.count = 1;
	VectorOperations::Set(result, Value(PI));
}

struct RandomBindData : public FunctionData {
	default_random_engine gen;
	uniform_real_distribution<double> dist;

	RandomBindData(default_random_engine gen, uniform_real_distribution<double> dist) : gen(gen), dist(dist) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RandomBindData>(gen, dist);
	}
};

void random_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                     Vector &result) {
	auto &info = (RandomBindData &) *expr.bind_info;
	assert(input_count == 0);
	result.Initialize(TypeId::DOUBLE);

	result.count = 1;
	if (exec.chunk) {
		result.count = exec.chunk->size();
		result.sel_vector = exec.chunk->sel_vector;
	}

	double *result_data = (double *) result.data;
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		result_data[i] = info.dist(info.gen);
	});
}

unique_ptr<FunctionData> random_bind(BoundFunctionExpression &expr, ClientContext &context) {
	random_device rd;
	default_random_engine gen(rd());
	uniform_real_distribution<double> dist(0, 1);
	return make_unique<RandomBindData>(move(gen), move(dist));
}

void sign_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	result.Initialize(TypeId::TINYINT);
	VectorOperations::Sign(inputs[0], result);
}

} // namespace duckdb
