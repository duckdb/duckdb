#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundComparisonExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.left.get());
	result->AddChild(expr.right.get());
	return result;
}

void ExpressionExecutor::Execute(BoundComparisonExpression &expr, ExpressionState *state, Vector &result) {
	// resolve the children
	Vector left(GetCardinality(), expr.left->return_type);
	Vector right(GetCardinality(), expr.right->return_type);
	Execute(*expr.left, state->child_states[0].get(), left);
	Execute(*expr.right, state->child_states[1].get(), right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException("Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}

idx_t ExpressionExecutor::Select(BoundComparisonExpression &expr, ExpressionState *state, sel_t result[]) {
	// resolve the children
	Vector left(GetCardinality(), expr.left->return_type);
	Vector right(GetCardinality(), expr.right->return_type);
	Execute(*expr.left, state->child_states[0].get(), left);
	Execute(*expr.right, state->child_states[1].get(), right);

	idx_t result_count;
	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		result_count = VectorOperations::SelectEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result_count = VectorOperations::SelectNotEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result_count = VectorOperations::SelectLessThan(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result_count = VectorOperations::SelectGreaterThan(left, right, result);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result_count = VectorOperations::SelectLessThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result_count = VectorOperations::SelectGreaterThanEquals(left, right, result);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException("Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
	return result_count;
}
