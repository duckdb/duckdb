#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

StatisticsPropagator::StatisticsPropagator(ClientContext &context) : context(context) {

}

bool StatisticsPropagator::PropagateStatistics(LogicalOperator &node) {
	switch(node.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return PropagateStatistics((LogicalFilter &) node);
	case LogicalOperatorType::LOGICAL_GET:
		return PropagateStatistics((LogicalGet &) node);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PropagateStatistics((LogicalProjection &) node);
	default:
		break;
	}
	for(idx_t child_idx = 0; child_idx < node.children.size(); child_idx++) {
		if (PropagateStatistics(*node.children[child_idx])) {
			// replace this child with an empty result
			node.children[child_idx] = make_unique<LogicalEmptyResult>(move(node.children[child_idx]));
		}
	}
	return false;
}


unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(Expression &expr) {
	switch(expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return PropagateExpression((BoundFunctionExpression &) expr);
	case ExpressionClass::BOUND_CAST:
		return PropagateExpression((BoundCastExpression &) expr);
	case ExpressionClass::BOUND_CONSTANT:
		return PropagateExpression((BoundConstantExpression &) expr);
	case ExpressionClass::BOUND_COLUMN_REF:
		return PropagateExpression((BoundColumnRefExpression &) expr);
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &expr) {
		PropagateExpression(expr);
	});
	return nullptr;
}


}

