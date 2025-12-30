#include "duckdb/common/assert.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

namespace {

struct ValueComparator {
	virtual ~ValueComparator() = default;
	virtual bool Compare(Value &lhs, Value &rhs) const = 0;
	virtual Value GetVal(BaseStatistics &stats) const = 0;
};

template <typename StatsType>
struct MinValueComp : public ValueComparator {
	bool Compare(Value &lhs, Value &rhs) const override {
		return lhs < rhs;
	}
	Value GetVal(BaseStatistics &stats) const override {
		return StatsType::Min(stats);
	}
};

template <typename StatsType>
struct MaxValueComp : public ValueComparator {
	bool Compare(Value &lhs, Value &rhs) const override {
		return lhs > rhs;
	}
	Value GetVal(BaseStatistics &stats) const override {
		return StatsType::Max(stats);
	}
};

template <typename StatsType>
unique_ptr<ValueComparator> GetComparator(const string &fun_name) {
	if (fun_name == "min") {
		return make_uniq<MinValueComp<StatsType>>();
	}
	D_ASSERT(fun_name == "max");
	return make_uniq<MaxValueComp<StatsType>>();
}

unique_ptr<ValueComparator> GetComparator(const string &fun_name, const LogicalType &type) {
	if (type == LogicalType::VARCHAR) {
		return GetComparator<StringStats>(fun_name);
	} else if (type.IsNumeric() || type.IsTemporal()) {
		return GetComparator<NumericStats>(fun_name);
	}
	return nullptr;
}

bool TryGetValueFromStats(const PartitionStatistics &stats, const StorageIndex &storage_index,
                          const ValueComparator &comparator, Value &result) {
	if (!stats.partition_row_group) {
		return false;
	}
	auto column_stats = stats.partition_row_group->GetColumnStatistics(storage_index);
	if (!stats.partition_row_group->MinMaxIsExact(*column_stats, storage_index)) {
		return false;
	}
	if (column_stats->GetStatsType() == StatisticsType::NUMERIC_STATS) {
		if (!NumericStats::HasMinMax(*column_stats)) {
			// TODO: This also returns if an entire row group is null. In that case, we could skip/compare null
			return false;
		}
	} else {
		D_ASSERT(column_stats->GetStatsType() == StatisticsType::STRING_STATS);
		if (StringStats::Min(*column_stats) > StringStats::Max(*column_stats)) {
			// No min/max statistics availabe
			return false;
		}
	}
	result = comparator.GetVal(*column_stats);
	return true;
}

} // namespace

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
			min_max_bindings.push_back(col_ref.binding);
			auto comparator = GetComparator(fun_name, col_ref.return_type);
			if (!comparator) {
				// Type has no min max statistics
				return;
			}
			comparators.push_back(std::move(comparator));
		} else if (fun_name == "count_star") {
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
	if (get.extra_info.sample_options) {
		// only use row group statistics if we query the whole table
		return;
	}

	// we can do the rewrite! get the stats
	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partition_stats = get.function.get_partition_stats(context, input);
	if (partition_stats.empty()) {
		// no partition stats found
		return;
	}

	vector<StorageIndex> min_max_storage_indexes(min_max_bindings.size());
	for (idx_t i = 0; i < min_max_bindings.size(); i++) {
		auto &binding = min_max_bindings[i];
		auto &column_index = get.GetColumnIds()[binding.column_index];
		if (!get.TryGetStorageIndex(column_index, min_max_storage_indexes[i])) {
			//! Can't get a storage index for this column, so it doesn't have stats we can use
			//! This happens when we're dealing with a generated column for example
			return;
		}
	}

	vector<LogicalType> types;
	vector<unique_ptr<Expression>> agg_results;
	// we can keep execute eager aggregate if all partitions could be either filtered entirely or remained entirely
	if (!get.table_filters.filters.empty()) {
		map<StorageIndex, reference<unique_ptr<TableFilter>>> filter_storage_index_map;
		for (auto &entry : get.table_filters.filters) {
			auto col_idx = entry.first;
			auto &filter = entry.second;
			auto column_index = ColumnIndex(col_idx);
			StorageIndex storage_index;
			if (!get.TryGetStorageIndex(column_index, storage_index)) {
				return;
			}
			filter_storage_index_map.emplace(storage_index, filter);
		}
		vector<PartitionStatistics> remaining_partition_stats;
		for (auto &stats : partition_stats) {
			if (!stats.partition_row_group) {
				return;
			}
			auto filter_result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
			for (auto &entry : filter_storage_index_map) {
				auto &storage_index = entry.first;
				auto &filter = entry.second;
				auto prg = stats.partition_row_group;
				if (!prg) {
					return;
				}
				auto column_stats = prg->GetColumnStatistics(storage_index);
				if (!column_stats) {
					return;
				}
				auto col_filter_result = filter.get()->CheckStatistics(*column_stats);
				if (col_filter_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
					// all data in this partition is filtered out, remove this partition entirely
					filter_result = FilterPropagateResult::FILTER_ALWAYS_FALSE;
					break;
				}
				if (col_filter_result != FilterPropagateResult::FILTER_ALWAYS_TRUE) {
					filter_result = col_filter_result;
				}
			}
			switch (filter_result) {
			case FilterPropagateResult::FILTER_ALWAYS_TRUE:
				// all filters passed - this partition should keep execute eager aggregate
				remaining_partition_stats.push_back(std::move(stats));
				break;
			case FilterPropagateResult::FILTER_ALWAYS_FALSE:
				break;
			default:
				// any filter that is not always true/false - bail
				return;
			}
		}
		partition_stats = std::move(remaining_partition_stats);
	}

	if (!min_max_bindings.empty()) {
		// Execute min/max aggregates on partition statistics
		for (idx_t agg_idx = 0; agg_idx < min_max_storage_indexes.size(); agg_idx++) {
			const auto &storage_index = min_max_storage_indexes[agg_idx];
			auto &comparator = comparators[agg_idx];

			Value agg_result;
			if (!TryGetValueFromStats(partition_stats[0], storage_index, *comparator, agg_result)) {
				return;
			}
			for (idx_t partition_idx = 1; partition_idx < partition_stats.size(); partition_idx++) {
				Value rhs;
				if (!TryGetValueFromStats(partition_stats[partition_idx], storage_index, *comparator, rhs)) {
					return;
				}
				if (!comparator->Compare(agg_result, rhs)) {
					agg_result = rhs;
				}
			}
			types.push_back(agg_result.GetTypeMutable());
			auto expr = make_uniq<BoundConstantExpression>(agg_result);
			agg_results.push_back(std::move(expr));
		}
	}
	if (!count_star_idxs.empty()) {
		// Execute count_star aggregates on partition statistics
		idx_t count = 0;
		for (const auto &stats : partition_stats) {
			if (stats.count_type == CountType::COUNT_APPROXIMATE) {
				// we cannot get an exact count
				return;
			}
			count += stats.count;
		}
		for (const auto count_star_idx : count_star_idxs) {
			auto count_result = make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(count)));
			agg_results.emplace(agg_results.begin() + NumericCast<int64_t>(count_star_idx), std::move(count_result));
			types.insert(types.begin() + NumericCast<int64_t>(count_star_idx), LogicalType::BIGINT);
		}
	}

	// Set column names
	for (idx_t expr_idx = 0; expr_idx < agg_results.size(); expr_idx++) {
		agg_results[expr_idx]->SetAlias(aggr.expressions[expr_idx]->GetAlias());
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
