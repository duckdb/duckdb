#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalLimit &limit, unique_ptr<LogicalOperator> *node_ptr)
{
	auto child = unique_ptr_cast<Operator, LogicalOperator>(std::move(limit.children[0]));
	// propagate statistics in the child node
	PropagateStatistics(child);
	limit.children[0] = std::move(child);
	// return the node stats, with as expected cardinality the amount specified in the limit
	return make_uniq<NodeStatistics>(limit.limit_val, limit.limit_val);
}

} // namespace duckdb
