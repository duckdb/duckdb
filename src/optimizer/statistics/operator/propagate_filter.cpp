#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

void StatisticsPropagator::PropagateStatistics(LogicalFilter &filter) {
	// first propagate to the child
	PropagateStatistics(*filter.children[0]);
	// then propagate to each of the expressions
	for(idx_t i = 0; i < filter.expressions.size(); i++) {
		PropagateExpression(*filter.expressions[i]);
	}
}

}
