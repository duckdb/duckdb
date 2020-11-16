#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalWindow &order, unique_ptr<LogicalOperator> *node_ptr) {
	// propagate statistics in the child node
	return PropagateStatistics(order.children[0]);
}

}
