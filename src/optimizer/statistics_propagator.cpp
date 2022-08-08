#include "duckdb/optimizer/statistics_propagator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

StatisticsPropagator::StatisticsPropagator(ClientContext &context) : context(context) {
}

void StatisticsPropagator::ReplaceWithEmptyResult(unique_ptr<LogicalOperator> &node) {
	node = make_unique<LogicalEmptyResult>(move(node));
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateChildren(LogicalOperator &node,
                                                                   unique_ptr<LogicalOperator> *node_ptr) {
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx++) {
		PropagateStatistics(node.children[child_idx]);
	}
	return nullptr;
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOperator &node,
                                                                     unique_ptr<LogicalOperator> *node_ptr) {
	switch (node.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return PropagateStatistics((LogicalAggregate &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PropagateStatistics((LogicalCrossProduct &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_FILTER:
		return PropagateStatistics((LogicalFilter &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_GET:
		return PropagateStatistics((LogicalGet &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PropagateStatistics((LogicalProjection &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PropagateStatistics((LogicalJoin &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return PropagateStatistics((LogicalSetOperation &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return PropagateStatistics((LogicalOrder &)node, node_ptr);
	case LogicalOperatorType::LOGICAL_WINDOW:
		return PropagateStatistics((LogicalWindow &)node, node_ptr);
	default:
		return PropagateChildren(node, node_ptr);
	}
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(unique_ptr<LogicalOperator> &node_ptr) {
	return PropagateStatistics(*node_ptr, &node_ptr);
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(Expression &expr,
                                                                     unique_ptr<Expression> *expr_ptr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE:
		return PropagateExpression((BoundAggregateExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_BETWEEN:
		return PropagateExpression((BoundBetweenExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_CASE:
		return PropagateExpression((BoundCaseExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_CONJUNCTION:
		return PropagateExpression((BoundConjunctionExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_FUNCTION:
		return PropagateExpression((BoundFunctionExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_CAST:
		return PropagateExpression((BoundCastExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_COMPARISON:
		return PropagateExpression((BoundComparisonExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_CONSTANT:
		return PropagateExpression((BoundConstantExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_COLUMN_REF:
		return PropagateExpression((BoundColumnRefExpression &)expr, expr_ptr);
	case ExpressionClass::BOUND_OPERATOR:
		return PropagateExpression((BoundOperatorExpression &)expr, expr_ptr);
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) { PropagateExpression(child); });
	return nullptr;
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(unique_ptr<Expression> &expr) {
	auto stats = PropagateExpression(*expr, &expr);
	if (ClientConfig::GetConfig(context).query_verification_enabled && stats) {
		expr->verification_stats = stats->Copy();
	}
	return stats;
}

} // namespace duckdb
