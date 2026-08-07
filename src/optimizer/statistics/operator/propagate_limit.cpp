#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalLimit &limit,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// propagate statistics in the child node
	auto child_stats = PropagateStatistics(limit.children[0]);

	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return nullptr;
	}

	auto has_no_offset =
	    limit.offset_val.Type() == LimitNodeType::UNSET ||
	    (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.offset_val.GetConstantValue() == 0);

	auto constant_limit = limit.limit_val.GetConstantValue();
	// Remove the limit if it cannot reduce the child cardinality.
	if (has_no_offset && child_stats && child_stats->has_max_cardinality &&
	    child_stats->max_cardinality <= constant_limit) {
		node_ptr = std::move(limit.children[0]);
		return child_stats;
	}

	// A constant limit provides an upper bound for the parent operators.
	auto result = make_uniq<NodeStatistics>(constant_limit, constant_limit);
	if (child_stats) {
		if (child_stats->has_estimated_cardinality) {
			result->estimated_cardinality = MinValue(constant_limit, child_stats->estimated_cardinality);
		}
		if (child_stats->has_max_cardinality) {
			result->max_cardinality = MinValue(constant_limit, child_stats->max_cardinality);
		}
	}
	return result;
}

} // namespace duckdb
