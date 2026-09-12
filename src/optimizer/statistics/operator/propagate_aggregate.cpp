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
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

namespace {

struct MinMaxColumnInfo {
	ColumnBinding binding;
	LogicalType input_type;
	LogicalType result_type;
};

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
	} else if (type.IsNumeric() || type.IsTemporal() || type.id() == LogicalTypeId::BOOLEAN) {
		return GetComparator<NumericStats>(fun_name);
	}
	return nullptr;
}

bool IsSafeMinMaxCast(const LogicalType &source, const LogicalType &target) {
	if (source == target) {
		return true;
	}
	if (!source.IsIntegral() || !target.IsIntegral()) {
		return false;
	}
	LogicalType max_type;
	return LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(source, target, max_type) && max_type == target;
}

bool TryGetMinMaxColumnInfo(const Expression &expr, MinMaxColumnInfo &info) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		const auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		info.binding = col_ref.Binding();
		info.input_type = col_ref.GetReturnType();
		info.result_type = col_ref.GetReturnType();
		return true;
	}
	if (!BoundCastExpression::IsCast(expr)) {
		return false;
	}
	const auto &cast = expr.Cast<BoundFunctionExpression>();
	const auto &cast_child = BoundCastExpression::Child(cast);
	if (cast_child.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &col_ref = cast_child.Cast<BoundColumnRefExpression>();
	if (!IsSafeMinMaxCast(col_ref.GetReturnType(), BoundCastExpression::TargetType(cast))) {
		return false;
	}
	info.binding = col_ref.Binding();
	info.input_type = col_ref.GetReturnType();
	info.result_type = BoundCastExpression::TargetType(cast);
	return true;
}

//! Outcome of reading one partition's statistics for a MIN/MAX aggregate.
enum class PartitionValueOutcome : uint8_t {
	//! The statistics are exact and produced a usable min/max value - the only foldable outcome
	VALUE,
	//! The partition holds only NULL values, which MIN/MAX ignore entirely - it is neutral
	ALL_NULL,
	//! The statistics bound every row of the partition reliably, but are not exact: `result` holds
	//! the bound. Good enough to vote with, never good enough to fold
	BOUND,
	//! No reliable state at all: the statistics do not describe the rows that will be read, or cannot
	//! be summarized
	NO_INFO
};

PartitionValueOutcome TryGetValueFromStats(const PartitionStatistics &stats, const StorageIndex &storage_index,
                                           const ValueComparator &comparator, const LogicalType &result_type,
                                           Value &result) {
	if (!stats.partition_row_group) {
		return PartitionValueOutcome::NO_INFO;
	}
	auto column_stats = stats.partition_row_group->GetColumnStatistics(storage_index);
	if (!column_stats) {
		return PartitionValueOutcome::NO_INFO;
	}
	if (stats.partition_row_group->HasPendingWrites()) {
		// rows appended to this partition locally are not covered by the statistics, so they are not
		// even a bound over the rows that will be read
		return PartitionValueOutcome::NO_INFO;
	}

	const bool is_numeric = column_stats->GetStatsType() == StatisticsType::NUMERIC_STATS;
	bool has_min_max;
	if (is_numeric) {
		has_min_max = NumericStats::HasMinMax(*column_stats);
	} else {
		D_ASSERT(column_stats->GetStatsType() == StatisticsType::STRING_STATS);
		has_min_max = StringStats::HasMinMax(*column_stats);
	}

	const bool min_max_exact = stats.partition_row_group->MinMaxIsExact(storage_index);
	if (!has_min_max) {
		if (!min_max_exact) {
			// with deleted rows in play the missing min/max says nothing about the surviving rows
			return PartitionValueOutcome::NO_INFO;
		}
		// A partition without min/max holds no non-null values at all. MIN/MAX ignore NULLs, so such
		// a partition is neutral rather than a reason to abandon the rewrite for the whole table.
		return column_stats->CanHaveNoNull() ? PartitionValueOutcome::NO_INFO : PartitionValueOutcome::ALL_NULL;
	}

	// the statistics bound the surviving rows; deleted rows and truncated string statistics make
	// them a bound instead of an exact value
	bool value_is_exact = min_max_exact;
	if (!is_numeric) {
		value_is_exact = value_is_exact && StringStats::GetMinType(*column_stats) == StringStatsType::EXACT_STATS &&
		                 StringStats::GetMaxType(*column_stats) == StringStatsType::EXACT_STATS;
	}

	result = comparator.GetVal(*column_stats);
	if (result.type() != result_type) {
		auto cast = result.DefaultTryCastAs(result_type);
		if (!cast) {
			// the value cannot be represented in the result type
			return PartitionValueOutcome::NO_INFO;
		}
		result = std::move(*cast);
	}
	return value_is_exact ? PartitionValueOutcome::VALUE : PartitionValueOutcome::BOUND;
}

//! Fold the MIN/MAX aggregates over the partition statistics, appending one constant per aggregate to
//! `types` and `agg_results` in the order of `storage_indexes`. Returns false if some partition's
//! statistics cannot answer an aggregate.
bool TryFoldMinMaxAggregates(const vector<PartitionStatistics> &partition_stats,
                             const vector<StorageIndex> &storage_indexes,
                             const vector<MinMaxColumnInfo> &min_max_columns,
                             const vector<unique_ptr<ValueComparator>> &comparators, vector<LogicalType> &types,
                             vector<unique_ptr<Expression>> &agg_results) {
	for (idx_t agg_idx = 0; agg_idx < storage_indexes.size(); agg_idx++) {
		const auto &storage_index = storage_indexes[agg_idx];
		const auto &result_type = min_max_columns[agg_idx].result_type;
		auto &comparator = comparators[agg_idx];

		Value agg_result;
		bool found_value = false;
		for (const auto &stats : partition_stats) {
			Value value;
			switch (TryGetValueFromStats(stats, storage_index, *comparator, result_type, value)) {
			case PartitionValueOutcome::VALUE:
				if (!found_value || !comparator->Compare(agg_result, value)) {
					agg_result = std::move(value);
					found_value = true;
				}
				break;
			case PartitionValueOutcome::ALL_NULL:
				// the partition holds no non-null values, so it cannot affect the extremum
				break;
			case PartitionValueOutcome::BOUND:
				// a bound always carries the value that bounds the partition
				D_ASSERT(!value.IsNull());
				// a bound is never exact: only an exact value can become a folded constant
				// TODO: a BOUND partition is meant to be handled by the scan-and-merge path once that
				// is reachable
				return false;
			case PartitionValueOutcome::NO_INFO:
				// the statistics cannot answer the aggregate
				return false;
			}
		}
		if (!found_value) {
			// every partition holds only NULLs - MIN/MAX over no non-null values is NULL
			agg_result = Value(result_type);
		}
		types.push_back(agg_result.type());
		auto expr = make_uniq<BoundConstantExpression>(agg_result);
		agg_results.push_back(std::move(expr));
	}
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

} // namespace

void StatisticsPropagator::TryExecuteAggregates(LogicalAggregate &aggr, unique_ptr<LogicalOperator> &node_ptr) {
	if (!aggr.groups.empty()) {
		// not possible with groups
		return;
	}
	// check if all aggregates are COUNT(*), MIN or MAX
	vector<idx_t> count_star_idxs;
	vector<MinMaxColumnInfo> min_max_columns;
	vector<unique_ptr<ValueComparator>> comparators;

	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		auto &aggr_ref = aggr.expressions[i];
		if (aggr_ref->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			// not an aggregate
			return;
		}
		auto &aggr_expr = aggr_ref->Cast<BoundAggregateExpression>();
		if (aggr_expr.GetFilter()) {
			// aggregate has a filter - bail
			return;
		}
		if (aggr_expr.StateExportMode() == AggregateStateExportMode::STATE_EXPORT) {
			// aggregate is in state export mode - cannot replace with a constant
			return;
		}
		auto &fun_name = aggr_expr.Function().GetName();
		if (fun_name == "min" || fun_name == "max") {
			if (aggr_expr.GetChildren().size() != 1) {
				return;
			}
			MinMaxColumnInfo column_info;
			if (!TryGetMinMaxColumnInfo(*aggr_expr.GetChildren()[0], column_info)) {
				return;
			}
			min_max_columns.push_back(column_info);
			auto comparator = GetComparator(fun_name, column_info.input_type);
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
		for (auto &column_info : min_max_columns) {
			auto &proj = child_ref.get().Cast<LogicalProjection>();
			auto &expr = proj.GetExpression(column_info.binding);
			MinMaxColumnInfo projection_info;
			if (!TryGetMinMaxColumnInfo(expr, projection_info)) {
				return;
			}
			if (!IsSafeMinMaxCast(projection_info.result_type, column_info.input_type)) {
				return;
			}
			column_info.binding = projection_info.binding;
			column_info.input_type = projection_info.input_type;
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

	vector<StorageIndex> min_max_storage_indexes(min_max_columns.size());
	for (idx_t i = 0; i < min_max_columns.size(); i++) {
		auto &binding = min_max_columns[i].binding;
		auto &column_index = get.GetColumnIndex(binding);
		if (!get.TryGetStorageIndex(column_index, min_max_storage_indexes[i])) {
			//! Can't get a storage index for this column, so it doesn't have stats we can use
			//! This happens when we're dealing with a generated column for example
			return;
		}
	}

	vector<LogicalType> types;
	vector<unique_ptr<Expression>> agg_results;
	bool need_to_scan = false;
	vector<idx_t> scan_partition_indices;
	// we can keep execute eager aggregate if all partitions could be either filtered entirely or remained entirely
	if (get.table_filters.HasFilters()) {
		map<StorageIndex, reference<TableFilter>> filter_storage_index_map;
		for (auto &entry : get.table_filters) {
			auto filter_idx = entry.GetIndex();
			auto &filter = entry.Filter();
			auto &column_index = get.GetColumnIndex(filter_idx);
			StorageIndex storage_index;
			if (!get.TryGetStorageIndex(column_index, storage_index)) {
				return;
			}
			filter_storage_index_map.emplace(storage_index, filter);
		}
		vector<PartitionStatistics> precomputed_partition_stats;
		for (idx_t partition_idx = 0; partition_idx < partition_stats.size(); partition_idx++) {
			auto &stats = partition_stats[partition_idx];
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
				if (!prg->MinMaxIsExact(storage_index) || prg->HasPendingWrites()) {
					filter_result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
					break;
				}
				auto &expr_filter =
				    ExpressionFilter::GetExpressionFilter(filter.get(), "AggregateStats::CheckPartitionFilters");
				auto col_filter_result = expr_filter.CheckStatistics(context, *column_stats);
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
				precomputed_partition_stats.push_back(std::move(stats));
				break;
			case FilterPropagateResult::FILTER_ALWAYS_FALSE:
				break;
			default:
				need_to_scan = true;
				scan_partition_indices.push_back(partition_idx);
				break;
			}
		}
		if (precomputed_partition_stats.empty()) {
			// no partitions can be pre-computed
			return;
		}
		partition_stats = std::move(precomputed_partition_stats);
	}

	if (partition_stats.empty()) {
		// no partitions can be pre-computed
		return;
	}

	if (!min_max_columns.empty()) {
		// Execute min/max aggregates on partition statistics
		if (!TryFoldMinMaxAggregates(partition_stats, min_max_storage_indexes, min_max_columns, comparators, types,
		                             agg_results)) {
			return;
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

	if (need_to_scan) {
		// Partial precomputation combines plan-time partition statistics with an execution-time scan that
		// skips partitions by their index in the row-group list. That list can change in between
		// (concurrent appends, checkpoints), in which case a skipped partition is scanned again and its
		// rows are counted twice. Only the full precomputation (no scan) is safe.
		return;
	}
	if (need_to_scan) {
		// Partial precomputation: some partitions need scanning
		// Insert a LogicalProjection above the aggregate that combines pre-computed constants with scan results
		if (!get.function.set_partitions_to_scan) {
			// scan does not support partition filtering - bail out
			return;
		}

		// Build projection expressions that merge pre-computed values with aggregate results
		auto proj_index = optimizer.binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> proj_expressions;
		for (idx_t i = 0; i < aggr.expressions.size(); i++) {
			auto &aggr_expr = aggr.expressions[i]->Cast<BoundAggregateExpression>();
			auto &fun_name = aggr_expr.Function().GetName();

			// Reference to the aggregate output column
			auto agg_col_ref = make_uniq<BoundColumnRefExpression>(
			    aggr_expr.GetReturnType(), ColumnBinding(aggr.aggregate_index, ProjectionIndex(i)));

			if (fun_name == "count_star") {
				// pre_count + count_star_from_scan
				auto &pre_count_expr = agg_results[i];
				auto add_expr = optimizer.BindScalarFunction("+", pre_count_expr->Copy(), std::move(agg_col_ref));
				add_expr->SetAlias(aggr.expressions[i]->GetAlias());
				proj_expressions.push_back(std::move(add_expr));
			} else if (fun_name == "min" || fun_name == "max") {
				// For min: COALESCE(least(pre_min, agg_min), pre_min)
				// For max: COALESCE(greatest(pre_max, agg_max), pre_max)
				auto &pre_val_expr = agg_results[i];
				Identifier merge_func((fun_name == "min") ? "least" : "greatest");
				auto merged = optimizer.BindScalarFunction(merge_func, pre_val_expr->Copy(), std::move(agg_col_ref));
				auto coalesce =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, aggr_expr.GetReturnType());
				coalesce->GetChildrenMutable().push_back(std::move(merged));
				coalesce->GetChildrenMutable().push_back(pre_val_expr->Copy());
				coalesce->SetAlias(aggr.expressions[i]->GetAlias());
				proj_expressions.push_back(std::move(coalesce));
			}
		}

		// Tell the scan to only scan partitions whose aggregates were NOT pre-computed
		get.SetPartitionsToScan(std::move(scan_partition_indices));

		// Create LogicalProjection above the aggregate
		auto projection = make_uniq<LogicalProjection>(proj_index, std::move(proj_expressions));
		projection->children.push_back(std::move(node_ptr));

		ColumnBindingReplacer replacer;
		for (idx_t i = 0; i < aggr.expressions.size(); i++) {
			auto old_binding = ColumnBinding(aggr.aggregate_index, ProjectionIndex(i));
			auto new_binding = ColumnBinding(proj_index, ProjectionIndex(i));
			replacer.replacement_bindings.emplace_back(old_binding, new_binding);
		}

		replacer.stop_operator = projection.get();
		node_ptr = std::move(projection);
		replacer.VisitOperator(*root);
		return;
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

	// propagate statistics in the aggregates
	for (idx_t aggregate_idx = 0; aggregate_idx < aggr.expressions.size(); aggregate_idx++) {
		auto &expr = aggr.expressions[aggregate_idx];

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
