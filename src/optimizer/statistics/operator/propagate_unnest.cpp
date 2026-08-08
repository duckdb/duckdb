#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalUnnest &unnest,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// propagate statistics to the child operator(s)
	for (idx_t child_idx = 0; child_idx < unnest.children.size(); child_idx++) {
		PropagateStatistics(unnest.children[child_idx]);
	}
	// also propagate the UNNEST expressions, so statistics-driven rewrites and callbacks (e.g. the lambda
	// functions' statistics callback on UNNEST(list_transform(...))) fire on them - the default operator handler
	// only recurses into child operators, not into the operator's own expressions
	for (idx_t i = 0; i < unnest.expressions.size(); i++) {
		PropagateExpression(unnest.expressions[i]);
	}
	// UNNEST expands each list to its length, so the output cardinality is input-dependent and unknown
	node_stats = nullptr;
	return nullptr;
}

} // namespace duckdb
