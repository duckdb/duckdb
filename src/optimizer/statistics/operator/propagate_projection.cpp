#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalProjection &proj,
                                                                     unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(proj.children[0]);
	if (proj.children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		ReplaceWithEmptyResult(*node_ptr);
		return std::move(node_stats);
	}

	// then propagate to each of the expressions
	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		auto stats = PropagateExpression(proj.expressions[i]);
		if (stats) {
			ColumnBinding binding(proj.table_index, i);
			statistics_map.insert(make_pair(binding, std::move(stats)));
		}
	}
	return std::move(node_stats);
}

} // namespace duckdb
