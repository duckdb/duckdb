
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOrder &order,
                                                                     unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(order.children[0]);

	// then propagate to each of the order expressions
	for (auto &bound_order : order.orders) {
		PropagateAndCompress(bound_order.expression, bound_order.stats);
	}
	return std::move(node_stats);
}

} // namespace duckdb
