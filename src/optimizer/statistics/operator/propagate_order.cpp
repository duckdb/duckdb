#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalOrder &order, unique_ptr<LogicalOperator> *node_ptr) {
	// propagate statistics in the child node
	return PropagateStatistics(order.children[0]);
}

}
