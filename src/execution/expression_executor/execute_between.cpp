#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

using namespace duckdb;
using namespace std;

struct BothInclusiveBetweenOperator {
	template <class T> static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct LowerInclusiveBetweenOperator {
	template <class T> static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

struct UpperInclusiveBetweenOperator {
	template <class T> static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct ExclusiveBetweenOperator {
	template <class T> static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

template <class OP>
static index_t between_loop_type_switch(Vector &input, Vector &lower, Vector &upper, sel_t result[]) {
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return TernaryExecutor::Select<int8_t, int8_t, int8_t, OP>(input, lower, upper, result);
	case TypeId::INT16:
		return TernaryExecutor::Select<int16_t, int16_t, int16_t, OP>(input, lower, upper, result);
	case TypeId::INT32:
		return TernaryExecutor::Select<int32_t, int32_t, int32_t, OP>(input, lower, upper, result);
	case TypeId::INT64:
		return TernaryExecutor::Select<int64_t, int64_t, int64_t, OP>(input, lower, upper, result);
	case TypeId::FLOAT:
		return TernaryExecutor::Select<float, float, float, OP>(input, lower, upper, result);
	case TypeId::DOUBLE:
		return TernaryExecutor::Select<double, double, double, OP>(input, lower, upper, result);
	case TypeId::VARCHAR:
		return TernaryExecutor::Select<const char *, const char *, const char *, OP>(input, lower, upper, result);
	default:
		throw InvalidTypeException(input.type, "Invalid type for BETWEEN");
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundBetweenExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.input.get());
	result->AddChild(expr.lower.get());
	result->AddChild(expr.upper.get());
	return result;
}

void ExpressionExecutor::Execute(BoundBetweenExpression &expr, ExpressionState *state, Vector &result) {
	// resolve the children
	Vector input(GetCardinality(), expr.input->return_type);
	Vector lower(GetCardinality(), expr.lower->return_type);
	Vector upper(GetCardinality(), expr.upper->return_type);
	Execute(*expr.input, state->child_states[0].get(), input);
	Execute(*expr.lower, state->child_states[1].get(), lower);
	Execute(*expr.upper, state->child_states[2].get(), upper);

	Vector intermediate1(GetCardinality(), TypeId::BOOL);
	Vector intermediate2(GetCardinality(), TypeId::BOOL);

	if (expr.upper_inclusive && expr.lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1);
		VectorOperations::LessThanEquals(input, upper, intermediate2);
	} else if (expr.lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1);
		VectorOperations::LessThan(input, upper, intermediate2);
	} else if (expr.upper_inclusive) {
		VectorOperations::GreaterThan(input, lower, intermediate1);
		VectorOperations::LessThanEquals(input, upper, intermediate2);
	} else {
		VectorOperations::GreaterThan(input, lower, intermediate1);
		VectorOperations::LessThan(input, upper, intermediate2);
	}
	VectorOperations::And(intermediate1, intermediate2, result);
}

index_t ExpressionExecutor::Select(BoundBetweenExpression &expr, ExpressionState *state, sel_t result[]) {
	// resolve the children
	Vector input(GetCardinality(), expr.input->return_type);
	Vector lower(GetCardinality(), expr.lower->return_type);
	Vector upper(GetCardinality(), expr.upper->return_type);
	Execute(*expr.input, state->child_states[0].get(), input);
	Execute(*expr.lower, state->child_states[1].get(), lower);
	Execute(*expr.upper, state->child_states[2].get(), upper);

	index_t result_count;
	if (expr.upper_inclusive && expr.lower_inclusive) {
		result_count = between_loop_type_switch<BothInclusiveBetweenOperator>(input, lower, upper, result);
	} else if (expr.lower_inclusive) {
		result_count = between_loop_type_switch<LowerInclusiveBetweenOperator>(input, lower, upper, result);
	} else if (expr.upper_inclusive) {
		result_count = between_loop_type_switch<UpperInclusiveBetweenOperator>(input, lower, upper, result);
	} else {
		result_count = between_loop_type_switch<ExclusiveBetweenOperator>(input, lower, upper, result);
	}
	return result_count;
}
