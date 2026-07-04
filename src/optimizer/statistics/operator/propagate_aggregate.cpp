#include "duckdb/common/assert.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include <cstdint>
#include <utility>

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
unique_ptr<ValueComparator> GetComparator(const Identifier &fun_name) {
	if (fun_name == "min") {
		return make_uniq<MinValueComp<StatsType>>();
	}
	D_ASSERT(fun_name == "max");
	return make_uniq<MaxValueComp<StatsType>>();
}

unique_ptr<ValueComparator> GetComparator(const Identifier &fun_name, const LogicalType &type) {
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
	if (!column_stats) {
		return false;
	}
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
		if (!StringStats::HasMinMax(*column_stats)) {
			return false;
		}
		if (StringStats::GetMinType(*column_stats) != StringStatsType::EXACT_STATS ||
		    StringStats::GetMaxType(*column_stats) != StringStatsType::EXACT_STATS) {
			// string stats are not exact - we cannot use the value from the stats
			return false;
		}
	}
	result = comparator.GetVal(*column_stats);
	return true;
}

bool GroupingSetCanIntroduceNull(const LogicalAggregate &aggr, idx_t group_idx) {
	if (aggr.grouping_sets.empty()) {
		return false;
	}
	const auto projection_idx = ProjectionIndex(group_idx);
	for (const auto &grouping_set : aggr.grouping_sets) {
		if (grouping_set.find(projection_idx) == grouping_set.end()) {
			return true;
		}
	}
	return false;
}

enum class PartitionAction : uint8_t { PRECOMPUTE, SCAN, PRUNE };

struct PartitionAggregateRequirements {
	//! filter column -> table filter (empty if the scan has no filters)
	map<StorageIndex, reference<TableFilter>> filters;
	//! columns that need an exact min/max, parallel to comparators
	vector<StorageIndex> min_max_columns;
	vector<unique_ptr<ValueComparator>> comparators;
	//! the query contains count(*)
	bool needs_exact_count = false;
};

struct PartitionAggregateInfo {
	optional_ptr<LogicalGet> get;
	PartitionAggregateRequirements requirements;
	//! positions of the aggregates in aggr.expressions
	vector<idx_t> count_star_idxs;
	vector<idx_t> min_max_idxs;
	//! return types of the min/max aggregates, parallel to min_max_idxs
	vector<LogicalType> min_max_types;
};

//! Values extracted from one partition, only valid when the action is PRECOMPUTE
struct AggregateValues {
	//! extracted min/max per column, parallel to requirements.min_max_columns
	vector<Value> min_max_values;
	idx_t count = 0;
};

bool TryGetPartitionAggregateInfo(LogicalAggregate &aggr, PartitionAggregateInfo &info) {
	if (!aggr.groups.empty()) {
		return false;
	}

	// every aggregate must be count_star, or min/max over a plain column
	vector<ColumnBinding> min_max_bindings;
	for (idx_t i = 0; i < aggr.expressions.size(); ++i) {
		auto &expr = aggr.expressions[i];
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &aggr_expr = expr->Cast<BoundAggregateExpression>();
		if (aggr_expr.GetFilter() || aggr_expr.StateExportMode() == AggregateStateExportMode::STATE_EXPORT) {
			return false;
		}
		auto &func_name = aggr_expr.Function().GetName();
		if (func_name == "min" || func_name == "max") {
			if (aggr_expr.GetChildren().size() != 1 ||
			    aggr_expr.GetChildren()[0]->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			auto &col_ref = aggr_expr.GetChildren()[0]->Cast<BoundColumnRefExpression>();
			auto comparator = GetComparator(func_name, col_ref.GetReturnType());
			if (!comparator) {
				// Type has no min max statistics
				return false;
			}
			min_max_bindings.push_back(col_ref.Binding());
			info.requirements.comparators.push_back(std::move(comparator));
			info.min_max_idxs.push_back(i);
			info.min_max_types.push_back(col_ref.GetReturnType());
		} else if (func_name == "count_star") {
			info.count_star_idxs.push_back(i);
			info.requirements.needs_exact_count = true;
		} else {
			return false;
		}
	}

	reference<LogicalOperator> child_ref = *aggr.children[0];
	while (child_ref.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = child_ref.get().Cast<LogicalProjection>();
		for (auto &binding : min_max_bindings) {
			auto &expr = proj.GetExpression(binding);
			if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}
			binding = expr.Cast<BoundColumnRefExpression>().Binding();
		}
		child_ref = *child_ref.get().children[0];
	}

	if (child_ref.get().type != LogicalOperatorType::LOGICAL_GET) {
		// child must be a LOGICAL_GET
		return false;
	}

	auto &get = child_ref.get().Cast<LogicalGet>();
	if (!get.function.get_partition_stats) {
		// GET does not support getting the partition stats
		return false;
	}
	if (get.extra_info.sample_options) {
		// only use row group statistics if we query the whole table
		return false;
	}

	auto &min_max_columns = info.requirements.min_max_columns;
	min_max_columns.resize(min_max_bindings.size());
	for (idx_t i = 0; i < min_max_bindings.size(); ++i) {
		auto &column_index = get.GetColumnIndex(min_max_bindings[i]);
		if (!get.TryGetStorageIndex(column_index, min_max_columns[i])) {
			// Can't get a storage index for this column, so it doesn't have stats we can use
			// This happens when we're dealing with a generated column for example
			return false;
		}
	}

	auto &filters = info.requirements.filters;
	for (auto &entry : get.table_filters) {
		auto &column_index = get.GetColumnIndex(entry.GetIndex());
		StorageIndex storage_index;
		if (!get.TryGetStorageIndex(column_index, storage_index)) {
			return false;
		}
		filters.emplace(storage_index, entry.Filter());
	}

	info.get = &get;

	return true;
}

PartitionAction ClassifyPartition(ClientContext &context, const PartitionStatistics &stats,
                                  const PartitionAggregateRequirements &requirements, AggregateValues &result) {
	auto &row_group = stats.partition_row_group;
	// filters can only be verified against per-partition column statistics;
	// without them the count/min-max checks below decide on their own
	if (!requirements.filters.empty() && !row_group) {
		return PartitionAction::SCAN;
	}
	// filters: trust ALWAYS_TRUE / ALWAYS_FALSE only when the filter column's
	// statistics are exact; a single trusted ALWAYS_FALSE prunes the partition
	bool partial_match = false;
	for (auto &[storage_index, filter] : requirements.filters) {
		auto column_stats = row_group->GetColumnStatistics(storage_index);
		if (!column_stats || !row_group->MinMaxIsExact(*column_stats, storage_index)) {
			partial_match = true;
			continue;
		}
		auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter.get(), "ClassifyPartition");
		switch (expr_filter.CheckStatistics(context, *column_stats)) {
		case FilterPropagateResult::FILTER_ALWAYS_FALSE:
			return PartitionAction::PRUNE;
		case FilterPropagateResult::FILTER_ALWAYS_TRUE:
			break;
		default:
			partial_match = true;
			break;
		}
	}

	if (partial_match) {
		return PartitionAction::SCAN;
	}

	if (requirements.needs_exact_count) {
		if (stats.count_type != CountType::COUNT_EXACT) {
			return PartitionAction::SCAN;
		}
		result.count = stats.count;
	}

	auto &min_max_columns = requirements.min_max_columns;
	auto &comparators = requirements.comparators;
	D_ASSERT(min_max_columns.size() == comparators.size());
	for (idx_t i = 0; i < min_max_columns.size(); ++i) {
		Value value;
		if (!TryGetValueFromStats(stats, min_max_columns[i], *comparators[i], value)) {
			return PartitionAction::SCAN;
		}
		result.min_max_values.push_back(std::move(value));
	}
	return PartitionAction::PRECOMPUTE;
}

} // namespace

void StatisticsPropagator::ReplaceWithPartialPrecompute(LogicalAggregate &aggr, LogicalGet &get,
                                                        vector<unique_ptr<Expression>> aggr_results,
                                                        vector<idx_t> scan_partition_indices,
                                                        unique_ptr<LogicalOperator> &node_ptr) {
	// merge each precomputed constant with the aggregate over the scanned partitions
	auto proj_index = optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_expressions;
	auto &aggr_expressions = aggr.expressions;
	for (idx_t i = 0; i < aggr_expressions.size(); ++i) {
		auto &aggr_expr = aggr_expressions[i]->Cast<BoundAggregateExpression>();
		auto &func_name = aggr_expr.Function().GetName();
		auto aggr_col_ref = make_uniq<BoundColumnRefExpression>(
		    aggr_expr.GetReturnType(), ColumnBinding(aggr.aggregate_index, ProjectionIndex(i)));

		if (func_name == "count_star") {
			// pre_count + count_star_from_scan
			auto &pre_count_expr = aggr_results[i];
			auto add_expr = optimizer.BindScalarFunction("+", pre_count_expr->Copy(), std::move(aggr_col_ref));
			add_expr->SetAlias(aggr.expressions[i]->GetAlias());
			proj_expressions.push_back(std::move(add_expr));
		} else {
			// For min: COALESCE(least(pre_min, agg_min), pre_min)
			// For max: COALESCE(greatest(pre_max, agg_max), pre_max)
			auto &pre_val_expr = aggr_results[i];
			Identifier merge_func((func_name == "min") ? "least" : "greatest");
			auto merged = optimizer.BindScalarFunction(merge_func, pre_val_expr->Copy(), std::move(aggr_col_ref));
			auto coalesce =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, aggr_expr.GetReturnType());
			coalesce->GetChildrenMutable().push_back(std::move(merged));
			coalesce->GetChildrenMutable().push_back(pre_val_expr->Copy());
			coalesce->SetAlias(aggr.expressions[i]->GetAlias());
			proj_expressions.push_back(std::move(coalesce));
		}
	}

	get.SetPartitionsToScan(std::move(scan_partition_indices));

	auto projection = make_uniq<LogicalProjection>(proj_index, std::move(proj_expressions));
	projection->children.push_back(std::move(node_ptr));
	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		replacer.replacement_bindings.emplace_back(ColumnBinding(aggr.aggregate_index, ProjectionIndex(i)),
		                                           ColumnBinding(proj_index, ProjectionIndex(i)));
	}
	replacer.stop_operator = projection.get();
	node_ptr = std::move(projection);
	replacer.VisitOperator(*root);
}

void StatisticsPropagator::ReplaceWithFullPrecompute(LogicalAggregate &aggr,
                                                     vector<unique_ptr<Expression>> aggr_results,
                                                     unique_ptr<LogicalOperator> &node_ptr) {
	D_ASSERT(aggr_results.size() == aggr.expressions.size());
	vector<LogicalType> types;
	for (idx_t i = 0; i < aggr_results.size(); i++) {
		aggr_results[i]->SetAlias(aggr.expressions[i]->GetAlias());
		types.push_back(aggr_results[i]->GetReturnType());
	}
	vector<vector<unique_ptr<Expression>>> expressions;
	expressions.push_back(std::move(aggr_results));
	auto expression_get =
	    make_uniq<LogicalExpressionGet>(aggr.aggregate_index, std::move(types), std::move(expressions));
	expression_get->children.push_back(make_uniq<LogicalDummyScan>(aggr.group_index));
	node_ptr = std::move(expression_get);
}

void StatisticsPropagator::TryExecuteAggregates(LogicalAggregate &aggr, unique_ptr<LogicalOperator> &node_ptr) {
	PartitionAggregateInfo info;
	if (!TryGetPartitionAggregateInfo(aggr, info)) {
		return;
	}
	auto &get = *info.get;
	GetPartitionStatsInput input(get.function, get.bind_data.get());
	auto partition_stats = get.function.get_partition_stats(context, input);
	if (partition_stats.empty()) {
		return;
	}

	// classify every partition, folding the precomputable ones as we go
	auto &reqs = info.requirements;
	idx_t precompute_count = 0;
	idx_t total_count = 0;
	vector<Value> min_max_values;
	for (auto &type : info.min_max_types) {
		min_max_values.emplace_back(type);
	}
	vector<idx_t> scan_partition_indices;

	for (idx_t partition_idx = 0; partition_idx < partition_stats.size(); partition_idx++) {
		AggregateValues values;
		switch (ClassifyPartition(context, partition_stats[partition_idx], reqs, values)) {
		case PartitionAction::PRECOMPUTE:
			precompute_count++;
			total_count += values.count;
			for (idx_t i = 0; i < min_max_values.size(); i++) {
				if (min_max_values[i].IsNull() ||
				    !reqs.comparators[i]->Compare(min_max_values[i], values.min_max_values[i])) {
					min_max_values[i] = values.min_max_values[i];
				}
			}
			break;
		case PartitionAction::SCAN:
			scan_partition_indices.push_back(partition_idx);
			break;
		case PartitionAction::PRUNE:
			break;
		}
	}

	if (precompute_count == 0) {
		return;
	}
	const bool need_to_scan = !scan_partition_indices.empty();
	if (need_to_scan && !get.function.set_partitions_to_scan) {
		return;
	}

	// build one constant per aggregate, in expression order
	vector<unique_ptr<Expression>> aggr_results(aggr.expressions.size());
	for (idx_t i = 0; i < info.min_max_idxs.size(); i++) {
		aggr_results[info.min_max_idxs[i]] = make_uniq<BoundConstantExpression>(min_max_values[i]);
	}
	for (const auto count_idx : info.count_star_idxs) {
		aggr_results[count_idx] = make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(total_count)));
	}

	if (need_to_scan) {
		ReplaceWithPartialPrecompute(aggr, get, std::move(aggr_results), std::move(scan_partition_indices), node_ptr);
		return;
	}

	// full precompute: replace the aggregate subtree with the constants
	ReplaceWithFullPrecompute(aggr, std::move(aggr_results), node_ptr);
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalAggregate &aggr,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate statistics in the child node
	node_stats = PropagateStatistics(aggr.children[0]);

	// handle the groups: simply propagate statistics and assign the stats to the group binding
	aggr.group_stats.resize(aggr.groups.size());
	for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
		auto stats = PropagateExpression(aggr.groups[group_idx]);
		if (stats && GroupingSetCanIntroduceNull(aggr, group_idx)) {
			stats->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		}
		aggr.group_stats[group_idx] = stats ? stats->ToUnique() : nullptr;
		if (!stats) {
			continue;
		}
		ColumnBinding group_binding(aggr.group_index, ProjectionIndex(group_idx));
		statistics_map[group_binding] = std::move(stats);
	}

	// Set up an expression matcher that detects COUNT(x)
	FunctionBinder function_binder(context);
	const auto count_fun = CountStarFun::GetFunction();
	const auto count_matcher = make_uniq<AggregateExpressionMatcher>();
	count_matcher->function = make_uniq<SpecificFunctionMatcher>("count");
	count_matcher->policy = SetMatcher::Policy::ORDERED;
	count_matcher->matchers.push_back(make_uniq<ExpressionMatcher>());

	// propagate statistics in the aggregates
	for (idx_t aggregate_idx = 0; aggregate_idx < aggr.expressions.size(); aggregate_idx++) {
		auto &expr = aggr.expressions[aggregate_idx];

		// Rewrite COUNT(x) to COUNT(*) if x cannot be NULL
		vector<reference<Expression>> bindings;
		if (count_matcher->Match(*expr, bindings)) {
			auto &aggr_expr = expr->Cast<BoundAggregateExpression>();
			const auto child_stats = PropagateExpression(aggr_expr.GetChildrenMutable()[0]);
			if (child_stats && !child_stats->CanHaveNull()) {
				expr = function_binder.BindAggregateFunction(count_fun, {}, nullptr, AggregateType::NON_DISTINCT);
			}
		}

		auto stats = PropagateExpression(expr);
		if (!stats) {
			continue;
		}
		ColumnBinding aggregate_binding(aggr.aggregate_index, ProjectionIndex(aggregate_idx));
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
		for (const auto &child : aggr_expr.GetChildren()) {
			if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				// Bail if bound aggregate child is not a colref
				distinct_validity = TupleDataValidityType::CAN_HAVE_NULL_VALUES;
				break;
			}
			const auto &col_ref = child->Cast<BoundColumnRefExpression>();
			auto it = statistics_map.find(col_ref.Binding());
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
