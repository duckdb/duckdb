#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundAggregateExpression &aggr,
                                                                     unique_ptr<Expression> *expr_ptr) {
	vector<unique_ptr<BaseStatistics>> stats;
	stats.reserve(aggr.children.size());
	for (auto &child : aggr.children) {
		stats.push_back(PropagateExpression(child));
	}
	if (!aggr.function.statistics) {
		return nullptr;
	}
	return aggr.function.statistics(context, aggr, aggr.bind_info.get(), stats, node_stats.get());
}

} // namespace duckdb
