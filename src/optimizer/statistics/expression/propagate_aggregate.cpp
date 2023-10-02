#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundAggregateExpression &aggr,
                                                                     unique_ptr<Expression> *expr_ptr) {
	vector<BaseStatistics> stats;
	stats.reserve(aggr.children.size());
	for (auto &child : aggr.children) {
		auto stat = PropagateExpression(child);
		if (!stat) {
			stats.push_back(BaseStatistics::CreateUnknown(child->return_type));
		} else {
			stats.push_back(stat->Copy());
		}
	}
	if (!aggr.function.statistics) {
		return nullptr;
	}
	AggregateStatisticsInput input(aggr.bind_info.get(), stats, node_stats.get());
	return aggr.function.statistics(context, aggr, input);
}

} // namespace duckdb
