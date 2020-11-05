#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

StatisticsPropagator::StatisticsPropagator(ClientContext &context) : context(context) {

}

void StatisticsPropagator::PropagateStatistics(LogicalOperator &node) {
	switch(node.type) {
	case LogicalOperatorType::FILTER:
		PropagateStatistics((LogicalFilter &) node);
		return;
	case LogicalOperatorType::GET:
		PropagateStatistics((LogicalGet &) node);
		return;
	case LogicalOperatorType::PROJECTION:
		PropagateStatistics((LogicalProjection &) node);
		return;
	default:
		break;
	}
	for(auto &child : node.children) {
		PropagateStatistics(*child);
	}
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

