#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/tuple_data_layout_enums.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

struct ValueComparator {
	virtual ~ValueComparator() = default;
	virtual Value &Compare(Value &lhs, Value &rhs) = 0;
	virtual Value GetVal(BaseStatistics &stats) = 0;
};

struct MinValueComp : public ValueComparator {
	Value &Compare(Value &lhs, Value &rhs) override {
		if (lhs < rhs) {
			return lhs;
		}
		return rhs;
	}
	Value GetVal(BaseStatistics &stats) override {
		return NumericStats::Min(stats);
	}
};

struct MaxValueComp : public ValueComparator {
	Value &Compare(Value &lhs, Value &rhs) override {
		if (lhs > rhs) {
			return lhs;
		}
		return rhs;
	}
	Value GetVal(BaseStatistics &stats) override {
		return NumericStats::Max(stats);
	}
};

unique_ptr<ValueComparator> GetComparator(const string &fun_name) {
	if (fun_name == "min") {
		return make_uniq<MinValueComp>();
	}
	D_ASSERT(fun_name == "max");
	return make_uniq<MaxValueComp>();
}

void StatisticsPropagator::TryExecuteAggregates(LogicalAggregate &aggr, unique_ptr<LogicalOperator> &node_ptr) {
	if (!aggr.groups.empty()) {
		// not possible with groups
		return;
	}
	// check if all aggregates are COUNT(*), MIN or MAX
	vector<idx_t> count_star_idxs;
	vector<ColumnBinding> min_max_bindings;
	vector<unique_ptr<ValueComparator>> comparators;

	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		auto &aggr_ref = aggr.expressions[i];
		if (aggr_ref->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			// not an aggregate
			return;
		}
		auto &aggr_expr = aggr_ref->Cast<BoundAggregateExpression>();
		if (aggr_expr.filter) {
			// aggregate has a filter - bail
			return;
		}
		const string &fun_name = aggr_expr.function.name;
		if (fun_name == "min" || fun_name == "max") {
			if (aggr_expr.children.size() != 1 || aggr_expr.children[0]->type != ExpressionType::BOUND_COLUMN_REF) {
				return;
			}
			const auto &col_ref = aggr_expr.children[0]->Cast<BoundColumnRefExpression>();
			if (!col_ref.return_type.IsNumeric() && !col_ref.return_type.IsTemporal()) {
				// TODO: If these are strings or some other fancy type, we might not have statistics
				return;
			}

			min_max_bindings.push_back(col_ref.binding);
			comparators.push_back(GetComparator(fun_name));
		} else if (aggr_expr.function.name == "count_star") {
			count_star_idxs.push_back(i);
		} else {
			// aggregate is not count star, min or max - bail
			return;
		}
	}

	// skip any projections
	reference<LogicalOperator> child_ref = *aggr.children[0];
	while (child_ref.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &binding : min_max_bindings) {
			auto &expr = child_ref.get().expressions[binding.column_index];
			if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
				return;
			}
			binding = expr->Cast<BoundColumnRefExpression>().binding;
		}
		child_ref = *child_ref.get().children[0];
	}

	if (child_ref.get().type != LogicalOperatorType::LOGICAL_GET) {
		// child must be a LOGICAL_GET
		return;
	}
	auto &get = child_ref.get().Cast<LogicalGet>();
	if (!get.function.get_partition_stats) {
		// GET does not support getting the partition stats
		return;
	}
	if (!get.table_filters.filters.empty()) {
		// we cannot do this if the GET has filters
		return;
	}

	// we can do the rewrite! get the stats
	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partition_stats = get.function.get_partition_stats(context, input);
	if (partition_stats.empty()) {
		// no partition stats found
		return;
	}
	idx_t count = 0;
	for (auto &stats : partition_stats) {
		if (stats.count_type == CountType::COUNT_APPROXIMATE) {
			// we cannot get an exact count
			return;
		}
		count += stats.count;
	}
	vector<Value> min_max_results;
	if (!min_max_bindings.empty()) {
		min_max_results.reserve(min_max_bindings.size());
		// TODO: combine this with previous loop
		for (idx_t partition_idx = 0; partition_idx < partition_stats.size(); partition_idx++) {
			auto &stats = partition_stats[partition_idx];
			if (!stats.partition_row_group) {
				return;
			}
			for (idx_t agg_idx = 0; agg_idx < min_max_bindings.size(); agg_idx++) {
				const auto &binding = min_max_bindings[agg_idx];
				const column_t column_index = get.GetColumnIds()[binding.column_index].GetPrimaryIndex();
				auto column_stats = stats.partition_row_group->GetColumnStatistics(column_index);
				if (!NumericStats::HasMinMax(*column_stats)) {
					// FIXME: This also returns if an entire rowgroup is null. In that case, we could skip/compare null
					return;
				}
				if (partition_idx == 0) {
					min_max_results.push_back(comparators[agg_idx]->GetVal(*column_stats));
				} else {
					auto rhs = comparators[agg_idx]->GetVal(*column_stats);
					min_max_results[agg_idx] = comparators[agg_idx]->Compare(min_max_results[agg_idx], rhs);
				}
			}
		}
	}

	// we got an exact count - replace the entire aggregate with a scan of the result
	vector<LogicalType> types;
	vector<unique_ptr<Expression>> agg_results;
	for (idx_t min_max_index = 0; min_max_index < min_max_results.size(); min_max_index++) {
		auto expr = make_uniq<BoundConstantExpression>(min_max_results[min_max_index]);
		// FIXME: Take name from aggregate expressions
		expr->SetAlias(get.names[min_max_bindings[min_max_index].column_index]);
		agg_results.push_back(std::move(expr));

		types.push_back(min_max_results[min_max_index].GetTypeMutable());
	}

	for (idx_t aggregate_index = 0; aggregate_index < count_star_idxs.size(); ++aggregate_index) {
		auto count_result = make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(count)));
		const idx_t count_star_idx = count_star_idxs[aggregate_index];
		count_result->SetAlias(aggr.expressions[count_star_idx]->GetName());

		agg_results.emplace(agg_results.begin() + NumericCast<int64_t>(count_star_idxs[aggregate_index]),
		                    std::move(count_result));

		types.push_back(LogicalType::BIGINT);
	}

	vector<vector<unique_ptr<Expression>>> expressions;
	expressions.push_back(std::move(agg_results));
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

	// check whether all inputs to the aggregate functions are valid
	TupleDataValidityType distinct_validity = TupleDataValidityType::CANNOT_HAVE_NULL_VALUES;
	for (const auto &aggr_ref : aggr.expressions) {
		if (distinct_validity == TupleDataValidityType::CAN_HAVE_NULL_VALUES) {
			break;
		}
		if (aggr_ref->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			// Bail if it's not a bound aggregate
			distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
			break;
		}
		auto &aggr_expr = aggr_ref->Cast<BoundAggregateExpression>();
		for (const auto &child : aggr_expr.children) {
			if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				// Bail if bound aggregate child is not a colref
				distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
				break;
			}
			const auto &col_ref = child->Cast<BoundColumnRefExpression>();
			auto it = statistics_map.find(col_ref.binding);
			if (it == statistics_map.end() || !it->second || it->second->CanHaveNull()) {
				// Bail if no stats or if there can be a NULL
				distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
				break;
			}
		}
	}
	aggr.distinct_validity = distinct_validity;

	// after we propagate statistics - try to directly execute aggregates using statistics
	TryExecuteAggregates(aggr, node_ptr);

	// the max cardinality of an aggregate is the max cardinality of the input (i.e. when every row is a unique
	// group)
	return std::move(node_stats);
}

} // namespace duckdb
