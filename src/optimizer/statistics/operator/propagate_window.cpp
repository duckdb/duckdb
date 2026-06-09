#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalWindow &window,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(window.children[0]);

	// then propagate to each of the order expressions
	for (auto &window_expr : window.expressions) {
		auto &over_expr = window_expr->Cast<BoundWindowExpression>();
		for (auto &expr : over_expr.PartitionsMutable()) {
			over_expr.PartitionsStatsMutable().push_back(PropagateExpression(expr));
		}
		for (auto &bound_order : over_expr.OrderByMutable()) {
			bound_order.stats = PropagateExpression(bound_order.expression);
		}

		if (over_expr.StartExpr()) {
			over_expr.ExprStatsMutable().push_back(PropagateExpression(over_expr.StartExprMutable()));
		} else {
			over_expr.ExprStatsMutable().push_back(nullptr);
		}

		if (over_expr.EndExpr()) {
			over_expr.ExprStatsMutable().push_back(PropagateExpression(over_expr.EndExprMutable()));
		} else {
			over_expr.ExprStatsMutable().push_back(nullptr);
		}

		for (auto &bound_order : over_expr.ArgOrdersMutable()) {
			bound_order.stats = PropagateExpression(bound_order.expression);
		}
	}
	return std::move(node_stats);
}

} // namespace duckdb
