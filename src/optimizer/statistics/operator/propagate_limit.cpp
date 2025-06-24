#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalLimit &limit,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// propagate statistics in the child node
	PropagateStatistics(limit.children[0]);
	// return the node stats, with as expected cardinality the amount specified in the limit
	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		auto constant_limit = limit.limit_val.GetConstantValue();
		return make_uniq<NodeStatistics>(constant_limit, constant_limit);
	}
	return nullptr;
}

} // namespace duckdb
