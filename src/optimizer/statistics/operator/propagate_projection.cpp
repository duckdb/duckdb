#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalProjection &proj, unique_ptr<LogicalOperator> *node_ptr)
{
	auto child = unique_ptr_cast<Operator, LogicalOperator>(std::move(proj.children[0]));
	// first propagate to the child
	node_stats = PropagateStatistics(child);
	proj.children[0] = std::move(child);
	if (proj.children[0]->logical_type == LogicalOperatorType::LOGICAL_EMPTY_RESULT)
	{
		ReplaceWithEmptyResult(*node_ptr);
		return std::move(node_stats);
	}
	// then propagate to each of the expressions
	for (idx_t i = 0; i < proj.expressions.size(); i++)
	{
		auto stats = PropagateExpression(proj.expressions[i]);
		if (stats)
		{
			ColumnBinding binding(proj.table_index, i);
			statistics_map.insert(make_pair(binding, std::move(stats)));
		}
	}
	return std::move(node_stats);
}
} // namespace duckdb