
#include <utility>
#include <vector>

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {
class LogicalOperator;

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOrder &order,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(order.children[0]);

	// then propagate to each of the order expressions
	for (auto &bound_order : order.orders) {
		bound_order.stats = PropagateExpression(bound_order.expression);
	}
	return std::move(node_stats);
}

} // namespace duckdb
