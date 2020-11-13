#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

StatisticsPropagator::StatisticsPropagator(ClientContext &context) : context(context) {

}

void StatisticsPropagator::ReplaceWithEmptyResult(unique_ptr<LogicalOperator> &node) {
	node = make_unique<LogicalEmptyResult>(move(node));
}

void StatisticsPropagator::PropagateChildren(LogicalOperator &node, unique_ptr<LogicalOperator> *node_ptr) {
	for(idx_t child_idx = 0; child_idx < node.children.size(); child_idx++) {
		PropagateStatistics(node.children[child_idx]);
	}
}

void StatisticsPropagator::PropagateStatistics(LogicalOperator &node, unique_ptr<LogicalOperator> *node_ptr) {
	switch(node.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		PropagateStatistics((LogicalFilter &) node, node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_GET:
		PropagateStatistics((LogicalGet &) node, node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		PropagateStatistics((LogicalProjection &) node, node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
		PropagateStatistics((LogicalJoin &) node, node_ptr);
		break;
	default:
		PropagateChildren(node, node_ptr);
		break;
	}
}

void StatisticsPropagator::PropagateStatistics(unique_ptr<LogicalOperator> &node_ptr) {
	PropagateStatistics(*node_ptr, &node_ptr);
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(Expression &expr, unique_ptr<Expression> *expr_ptr) {
	switch(expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return PropagateExpression((BoundFunctionExpression &) expr, expr_ptr);
	case ExpressionClass::BOUND_CAST:
		return PropagateExpression((BoundCastExpression &) expr, expr_ptr);
	case ExpressionClass::BOUND_COMPARISON:
		return PropagateExpression((BoundComparisonExpression &) expr, expr_ptr);
	case ExpressionClass::BOUND_CONSTANT:
		return PropagateExpression((BoundConstantExpression &) expr, expr_ptr);
	case ExpressionClass::BOUND_COLUMN_REF:
		return PropagateExpression((BoundColumnRefExpression &) expr, expr_ptr);
	case ExpressionClass::BOUND_OPERATOR:
		return PropagateExpression((BoundOperatorExpression &) expr, expr_ptr);
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> child) {
		PropagateExpression(child);
		return child;
	});
	return nullptr;
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(unique_ptr<Expression> &expr) {
	return PropagateExpression(*expr, &expr);

}


}

