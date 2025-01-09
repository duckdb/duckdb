#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

void StatisticsPropagator::TryExecuteAggregates(LogicalAggregate &aggr, unique_ptr<LogicalOperator> &node_ptr) {
	if (!aggr.groups.empty()) {
		// not possible with groups
		return;
	}
	if (aggr.children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		// child must be a LOGICAL_GET
		// FIXME: we could do this with projections as well
		return;
	}
	auto &get = aggr.children[0]->Cast<LogicalGet>();
	if (!get.function.get_partition_stats) {
		// GET does not support getting the partition stats
		return;
	}
	if (!get.table_filters.filters.empty()) {
		// we cannot do this if the GET has filters
		return;
	}
	// check if all aggregates are COUNT(*)
	for (auto &aggr_ref : aggr.expressions) {
		if (aggr_ref->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			// not an aggregate
			return;
		}
		auto &aggr_expr = aggr_ref->Cast<BoundAggregateExpression>();
		if (aggr_expr.function.name != "count_star") {
			// aggregate is not count star - bail
			return;
		}
		if (aggr_expr.filter) {
			// aggregate has a filter - bail
			return;
		}
	}
	// we can do the rewrite! get the stats
	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partition_stats = get.function.get_partition_stats(context, input);
	idx_t count = 0;
	for (auto &stats : partition_stats) {
		if (stats.count_type == CountType::COUNT_APPROXIMATE) {
			// we cannot get an exact count
			return;
		}
		count += stats.count;
	}
	// we got an exact count - replace the entire aggregate with a scan of the result
	vector<LogicalType> types;
	vector<unique_ptr<Expression>> count_results;
	for (idx_t aggregate_index = 0; aggregate_index < aggr.expressions.size(); ++aggregate_index) {
		auto count_result = make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(count)));
		count_result->SetAlias(aggr.expressions[aggregate_index]->GetName());
		count_results.push_back(std::move(count_result));

		types.push_back(LogicalType::BIGINT);
	}

	vector<vector<unique_ptr<Expression>>> expressions;
	expressions.push_back(std::move(count_results));
	auto expression_get =
	    make_uniq<LogicalExpressionGet>(aggr.aggregate_index, std::move(types), std::move(expressions));
	expression_get->children.push_back(make_uniq<LogicalDummyScan>(aggr.group_index));
	node_ptr = std::move(expression_get);
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalAggregate &aggr,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate statistics in the child node
	node_stats = PropagateStatistics(aggr.children[0]);

	// handle the groups: simply propagate statistics and assign the stats to the group binding
	aggr.group_stats.resize(aggr.groups.size());
	for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
		auto stats = PropagateExpression(aggr.groups[group_idx]);
		aggr.group_stats[group_idx] = stats ? stats->ToUnique() : nullptr;
		if (!stats) {
			continue;
		}
		if (aggr.grouping_sets.size() > 1) {
			// aggregates with multiple grouping sets can introduce NULL values to certain groups
			// FIXME: actually figure out WHICH groups can have null values introduced
			stats->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
			continue;
		}
		ColumnBinding group_binding(aggr.group_index, group_idx);
		statistics_map[group_binding] = std::move(stats);
	}
	// propagate statistics in the aggregates
	for (idx_t aggregate_idx = 0; aggregate_idx < aggr.expressions.size(); aggregate_idx++) {
		auto stats = PropagateExpression(aggr.expressions[aggregate_idx]);
		if (!stats) {
			continue;
		}
		ColumnBinding aggregate_binding(aggr.aggregate_index, aggregate_idx);
		statistics_map[aggregate_binding] = std::move(stats);
	}

	// after we propagate statistics - try to directly execute aggregates using statistics
	TryExecuteAggregates(aggr, node_ptr);

	// the max cardinality of an aggregate is the max cardinality of the input (i.e. when every row is a unique group)
	return std::move(node_stats);
}

} // namespace duckdb
