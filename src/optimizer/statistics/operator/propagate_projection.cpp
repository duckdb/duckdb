#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

void StatisticsPropagator::PropagateStatistics(LogicalProjection &proj, unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate to the child
	PropagateStatistics(proj.children[0]);

	// then propagate to each of the expressions
	for(idx_t i = 0; i < proj.expressions.size(); i++) {
		auto stats = PropagateExpression(proj.expressions[i]);
		if (stats) {
			ColumnBinding binding(proj.table_index, i);
			statistics_map.insert(make_pair(binding, move(stats)));
		}
	}
}

}
