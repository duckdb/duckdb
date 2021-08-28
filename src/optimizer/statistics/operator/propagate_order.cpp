#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOrder &order,
                                                                     unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(order.children[0]);

	// then propagate to each of the order expressions
	for (auto &bound_order : order.orders) {
		auto &expr = bound_order.expression;
		PropagateExpression(expr);
		if (expr->stats) {
			bound_order.stats = expr->stats->Copy();
		} else {
			bound_order.stats = nullptr;
		}
	}
	return move(node_stats);
}

} // namespace duckdb
