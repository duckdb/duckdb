#include <vector>

#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundAggregateExpression &aggr,
                                                                     unique_ptr<Expression> &expr_ptr) {
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
	if (!aggr.function.HasStatisticsCallback()) {
		return nullptr;
	}
	AggregateStatisticsInput input(aggr.bind_info.get(), stats, node_stats.get());
	return aggr.function.GetStatisticsCallback()(context, aggr, input);
}

} // namespace duckdb
