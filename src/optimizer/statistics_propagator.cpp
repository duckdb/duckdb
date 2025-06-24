#include "duckdb/optimizer/statistics_propagator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/compressed_materialization.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

StatisticsPropagator::StatisticsPropagator(Optimizer &optimizer_p, LogicalOperator &root_p)
    : optimizer(optimizer_p), context(optimizer.context), root(&root_p) {
	root->ResolveOperatorTypes();
}

void StatisticsPropagator::ReplaceWithEmptyResult(unique_ptr<LogicalOperator> &node) {
	node = make_uniq<LogicalEmptyResult>(std::move(node));
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateChildren(LogicalOperator &node,
                                                                   unique_ptr<LogicalOperator> &node_ptr) {
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx++) {
		PropagateStatistics(node.children[child_idx]);
	}
	return nullptr;
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOperator &node,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	unique_ptr<NodeStatistics> result;
	switch (node.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		result = PropagateStatistics(node.Cast<LogicalAggregate>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		result = PropagateStatistics(node.Cast<LogicalCrossProduct>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		result = PropagateStatistics(node.Cast<LogicalFilter>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_GET:
		result = PropagateStatistics(node.Cast<LogicalGet>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		result = PropagateStatistics(node.Cast<LogicalProjection>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		result = PropagateStatistics(node.Cast<LogicalJoin>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		result = PropagateStatistics(node.Cast<LogicalPositionalJoin>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		result = PropagateStatistics(node.Cast<LogicalSetOperation>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		result = PropagateStatistics(node.Cast<LogicalOrder>(), node_ptr);
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		result = PropagateStatistics(node.Cast<LogicalWindow>(), node_ptr);
		break;
	default:
		result = PropagateChildren(node, node_ptr);
	}

	if (!optimizer.OptimizerDisabled(OptimizerType::COMPRESSED_MATERIALIZATION)) {
		// compress data based on statistics for materializing operators
		CompressedMaterialization compressed_materialization(optimizer, *root, statistics_map);
		compressed_materialization.Compress(node_ptr);
	}

	return result;
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(unique_ptr<LogicalOperator> &node_ptr) {
	return PropagateStatistics(*node_ptr, node_ptr);
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(Expression &expr,
                                                                     unique_ptr<Expression> &expr_ptr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE:
		return PropagateExpression(expr.Cast<BoundAggregateExpression>(), expr_ptr);
	case ExpressionClass::BOUND_BETWEEN:
		return PropagateExpression(expr.Cast<BoundBetweenExpression>(), expr_ptr);
	case ExpressionClass::BOUND_CASE:
		return PropagateExpression(expr.Cast<BoundCaseExpression>(), expr_ptr);
	case ExpressionClass::BOUND_CONJUNCTION:
		return PropagateExpression(expr.Cast<BoundConjunctionExpression>(), expr_ptr);
	case ExpressionClass::BOUND_FUNCTION:
		return PropagateExpression(expr.Cast<BoundFunctionExpression>(), expr_ptr);
	case ExpressionClass::BOUND_CAST:
		return PropagateExpression(expr.Cast<BoundCastExpression>(), expr_ptr);
	case ExpressionClass::BOUND_COMPARISON:
		return PropagateExpression(expr.Cast<BoundComparisonExpression>(), expr_ptr);
	case ExpressionClass::BOUND_CONSTANT:
		return PropagateExpression(expr.Cast<BoundConstantExpression>(), expr_ptr);
	case ExpressionClass::BOUND_COLUMN_REF:
		return PropagateExpression(expr.Cast<BoundColumnRefExpression>(), expr_ptr);
	case ExpressionClass::BOUND_OPERATOR:
		return PropagateExpression(expr.Cast<BoundOperatorExpression>(), expr_ptr);
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) { PropagateExpression(child); });
	return nullptr;
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(unique_ptr<Expression> &expr) {
	auto stats = PropagateExpression(*expr, expr);
	if (ClientConfig::GetConfig(context).query_verification_enabled && stats) {
		expr->verification_stats = stats->ToUnique();
	}
	return stats;
}

} // namespace duckdb
