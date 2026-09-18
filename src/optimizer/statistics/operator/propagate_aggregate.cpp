#include "duckdb/common/assert.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/arg_properties.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/partition_fold.hpp"
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
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

namespace {

struct MinMaxColumnInfo {
	ColumnBinding binding;
	LogicalType input_type;
	LogicalType result_type;
	//! Whether the aggregate is a MIN, and where it sits in aggr.expressions
	bool is_min;
	idx_t aggr_idx;
};

struct ValueComparator {
	virtual ~ValueComparator() = default;
	virtual bool Compare(const Value &lhs, const Value &rhs) const = 0;
	virtual Value GetVal(BaseStatistics &stats) const = 0;
};

template <typename StatsType>
struct MinValueComp : public ValueComparator {
	bool Compare(const Value &lhs, const Value &rhs) const override {
		return lhs < rhs;
	}
	Value GetVal(BaseStatistics &stats) const override {
		return StatsType::Min(stats);
	}
};

template <typename StatsType>
struct MaxValueComp : public ValueComparator {
	bool Compare(const Value &lhs, const Value &rhs) const override {
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

enum class LengthFunctionKind : uint8_t { BYTE_LENGTH, CHARACTER_LENGTH };

//! A recognized MIN/MAX over a length function of a string column.
struct LengthColumnInfo {
	ColumnBinding binding;
	//! A copy of strlen/octet_length/length; the statistics callback maps string length fields
	unique_ptr<Expression> function;
	LengthFunctionKind kind;
	bool is_min;
	idx_t aggr_idx;
};

//! Column under strlen(VARCHAR), octet_length(BLOB), or length/char_length(VARCHAR).
bool TryGetLengthColumnRef(const Expression &expr, LengthColumnInfo &info) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &fun = expr.Cast<BoundFunctionExpression>();
	if (fun.GetChildren().size() != 1 ||
	    fun.GetChildren()[0]->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	const auto &fun_name = fun.Function().GetName();
	const auto arg_type = fun.GetChildren()[0]->GetReturnType().id();
	LengthFunctionKind detected_kind;
	if (fun_name == "strlen" && arg_type == LogicalTypeId::VARCHAR) {
		detected_kind = LengthFunctionKind::BYTE_LENGTH;
	} else if (fun_name == "octet_length") {
		// octet_length(BIT) is GetSize()-1; string stats store GetSize()
		if (arg_type != LogicalTypeId::BLOB) {
			return false;
		}
		detected_kind = LengthFunctionKind::BYTE_LENGTH;
	} else if (fun_name == "length" || fun_name == "len" || fun_name == "char_length" ||
	           fun_name == "character_length") {
		if (arg_type != LogicalTypeId::VARCHAR) {
			return false;
		}
		detected_kind = LengthFunctionKind::CHARACTER_LENGTH;
	} else {
		return false;
	}
	if (!fun.Function().HasStatisticsCallback()) {
		return false;
	}
	info.binding = fun.GetChildren()[0]->Cast<BoundColumnRefExpression>().Binding();
	info.function = expr.Copy();
	info.kind = detected_kind;
	return true;
}

//! A recognized MIN/MAX over a monotone function of a column: MIN/MAX(f(col))
struct MonotoneColumnInfo {
	ColumnBinding binding;
	//! A copy of `f(...)`; evaluating it needs arg_values spliced in
	unique_ptr<Expression> function;
	//! The constant arguments, indexed like the function's children
	vector<Value> arg_values;
	//! The argument that holds the column
	idx_t column_arg;
	//! Whether f is non-increasing in that argument
	bool decreasing;
	bool is_min;
	idx_t aggr_idx;
};

//! Resolve MIN/MAX(f(col)): one column arg with known monotonicity; other args constant.
bool TryGetMonotoneColumnInfo(ClientContext &context, const Expression &expr, MonotoneColumnInfo &info) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (!func.Function().HasArgProperties() || func.Function().GetStability() != FunctionStability::CONSISTENT) {
		// without an annotation there is no reason to believe the endpoints map to the extremes
		return false;
	}
	if (BaseStatistics::GetStatsType(func.GetReturnType()) != StatisticsType::NUMERIC_STATS ||
	    func.GetReturnType().InternalType() == PhysicalType::BOOL) {
		// the mapped value has to be orderable for MIN/MAX over it to be defined
		return false;
	}
	optional_idx column_arg;
	vector<Value> arg_values(func.GetChildren().size());
	bool decreasing = false;
	for (idx_t i = 0; i < func.GetChildren().size(); i++) {
		auto &child = *func.GetChildren()[i];
		if (child.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			if (!child.IsFoldable()) {
				return false;
			}
			if (!ExpressionExecutor::TryEvaluateScalar(context, child, arg_values[i]) || arg_values[i].IsNull()) {
				return false;
			}
			continue;
		}
		if (column_arg.IsValid()) {
			return false;
		}
		const auto monotonicity = func.Function().GetArgProperties(i).monotonicity;
		if (!IsKnownMonotonic(monotonicity) || monotonicity == Monotonicity::CONSTANT) {
			return false;
		}
		if (child.GetReturnType().id() == LogicalTypeId::INTERVAL) {
			// intervals are not totally ordered, so an interval endpoint carries no extremum
			return false;
		}
		column_arg = i;
		decreasing = IsMonotonicDecreasing(monotonicity);
		info.binding = child.Cast<BoundColumnRefExpression>().Binding();
	}
	if (!column_arg.IsValid()) {
		// no column to read the endpoints from
		return false;
	}
	info.function = expr.Copy();
	info.arg_values = std::move(arg_values);
	info.column_arg = column_arg.GetIndex();
	info.decreasing = decreasing;
	return true;
}

//! Extract the partition's extremum for the aggregate, cast to the result type. Returns false when
//! the value cannot be represented.
static bool ExtractMinMaxValue(BaseStatistics &column_stats, const ValueComparator &comparator,
                               const LogicalType &result_type, Value &result) {
	result = comparator.GetVal(column_stats);
	if (result.type() != result_type) {
		auto cast = result.DefaultTryCastAs(result_type);
		if (!cast) {
			// the value cannot be represented in the result type
			return false;
		}
		result = std::move(*cast);
	}
	return true;
}

//! MIN/MAX over the partition statistics. Exact partitions are reduced into a candidate, partitions
//! holding only NULL values are neutral; when every partition is neutral the result is NULL.
struct MinMaxFoldClient {
	MinMaxFoldClient(MinMaxColumnInfo column_info_p, unique_ptr<ValueComparator> comparator_p,
	                 StorageIndex storage_index_p)
	    : column_info(std::move(column_info_p)), comparator(std::move(comparator_p)),
	      storage_index(std::move(storage_index_p)) {
	}

	//! Classify one partition for this aggregate. On EXACT_VALUE and BOUND, `value` holds the value
	//! respectively the bound.
	FoldPartitionState ClassifyPartition(const FoldPartition &partition, Value &value) const {
		auto &stats = partition.stats;
		if (!stats.partition_row_group) {
			return FoldPartitionState::NO_INFO;
		}
		auto column_stats = stats.partition_row_group->GetColumnStatistics(storage_index);
		if (!column_stats) {
			return FoldPartitionState::NO_INFO;
		}
		if (stats.partition_row_group->HasPendingWrites()) {
			// rows appended to this partition locally are not covered by the statistics, so they are
			// not even a bound over the rows that will be read
			return FoldPartitionState::NO_INFO;
		}

		const bool is_numeric = column_stats->GetStatsType() == StatisticsType::NUMERIC_STATS;
		bool has_min_max;
		if (is_numeric) {
			has_min_max = NumericStats::HasMinMax(*column_stats);
		} else {
			D_ASSERT(column_stats->GetStatsType() == StatisticsType::STRING_STATS);
			has_min_max = StringStats::HasMinMax(*column_stats);
		}

		if (partition.filter_result != FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			// the filter cuts the partition: it only removes rows, so the all-rows statistics remain a
			// superset bound over the surviving rows without ever being attained by one of them
			if (!has_min_max) {
				// the filter cannot turn a partition without non-null values into one with them
				return column_stats->CanHaveNoNull() ? FoldPartitionState::NO_INFO : FoldPartitionState::NEUTRAL;
			}
			return ExtractMinMaxValue(*column_stats, *comparator, column_info.result_type, value)
			           ? FoldPartitionState::BOUND
			           : FoldPartitionState::NO_INFO;
		}

		const bool min_max_exact = stats.partition_row_group->MinMaxIsExact(storage_index);
		if (!has_min_max) {
			if (!min_max_exact) {
				// with deleted rows in play the missing min/max says nothing about the surviving rows
				return FoldPartitionState::NO_INFO;
			}
			// A partition without min/max holds no non-null values at all. MIN/MAX ignore NULLs, so
			// such a partition is neutral rather than a reason to abandon the rewrite for the whole
			// table.
			return column_stats->CanHaveNoNull() ? FoldPartitionState::NO_INFO : FoldPartitionState::NEUTRAL;
		}

		// the statistics bound the surviving rows; deleted rows and truncated string statistics make
		// them a bound instead of an exact value
		bool value_is_exact = min_max_exact;
		if (!is_numeric) {
			value_is_exact = value_is_exact && StringStats::GetMinType(*column_stats) == StringStatsType::EXACT_STATS &&
			                 StringStats::GetMaxType(*column_stats) == StringStatsType::EXACT_STATS;
		}

		if (!ExtractMinMaxValue(*column_stats, *comparator, column_info.result_type, value)) {
			return FoldPartitionState::NO_INFO;
		}
		return value_is_exact ? FoldPartitionState::EXACT_VALUE : FoldPartitionState::BOUND;
	}

	void CombineCandidate(Value &candidate, Value &value) const {
		if (comparator->Compare(value, candidate)) {
			candidate = std::move(value);
		}
	}

	bool ExcludesCandidate(const Value &bound, const Value &candidate) const {
		if (column_info.input_type == LogicalType::VARCHAR) {
			// string statistics may keep only a truncated prefix of the value: the bound does not
			// upper-bound the true maximum, so a plain comparison cannot exclude the partition
			return false;
		}
		// the partition is excluded when its bound is weakly dominated by the candidate: a surviving
		// row changes the candidate only if it compares strictly better than it
		return !comparator->Compare(bound, candidate);
	}

	Value FallbackValue() const {
		// MIN/MAX over no non-null values is NULL
		return Value(column_info.result_type);
	}

	MinMaxColumnInfo column_info;
	unique_ptr<ValueComparator> comparator;
	StorageIndex storage_index;
};

//! COUNT(*) over the partition statistics: the partition counts must be exact and are summed.
struct CountStarFoldClient {
	FoldPartitionState ClassifyPartition(const FoldPartition &partition, Value &value) const {
		if (partition.filter_result != FilterPropagateResult::FILTER_ALWAYS_TRUE ||
		    partition.stats.count_type == CountType::COUNT_APPROXIMATE) {
			// we cannot get an exact count: the surviving row count of a partition the filter cuts
			// is unknown, and an approximate total is just as unusable
			return FoldPartitionState::NO_INFO;
		}
		value = Value::BIGINT(NumericCast<int64_t>(partition.stats.count));
		return FoldPartitionState::EXACT_VALUE;
	}

	void CombineCandidate(Value &candidate, Value &value) const {
		candidate = Value::BIGINT(candidate.GetValue<int64_t>() + value.GetValue<int64_t>());
	}

	bool ExcludesCandidate(const Value &, const Value &) const {
		// a count has no bound source - every partition must be exact
		return false;
	}

	Value FallbackValue() const {
		return Value::BIGINT(0);
	}
};

//! MIN/MAX over a length function of a string column. Byte length and ASCII character length are
//! attained values; unicode character length is only a code-point bound from LengthPropagateStats.
struct LengthFoldClient {
	LengthFoldClient(ClientContext &context_p, unique_ptr<Expression> function_p, StorageIndex storage_index_p,
	                 bool is_min_p, LengthFunctionKind kind_p, LogicalType result_type_p)
	    : context(context_p), function(std::move(function_p)), storage_index(std::move(storage_index_p)),
	      is_min(is_min_p), kind(kind_p), result_type(std::move(result_type_p)) {
	}

	FoldPartitionState ClassifyPartition(const FoldPartition &partition, Value &value) const {
		auto &stats = partition.stats;
		if (!stats.partition_row_group) {
			return FoldPartitionState::NO_INFO;
		}
		auto column_stats = stats.partition_row_group->GetColumnStatistics(storage_index);
		if (!column_stats) {
			return FoldPartitionState::NO_INFO;
		}
		if (stats.partition_row_group->HasPendingWrites()) {
			// rows appended to this partition locally are not covered by the statistics
			return FoldPartitionState::NO_INFO;
		}
		if (column_stats->GetStatsType() != StatisticsType::STRING_STATS) {
			return FoldPartitionState::NO_INFO;
		}
		if (!column_stats->CanHaveNoNull()) {
			// the partition holds only NULL values: MIN/MAX ignores them
			return FoldPartitionState::NEUTRAL;
		}
		if (!TryGetLengthValue(*column_stats, value)) {
			return FoldPartitionState::NO_INFO;
		}
		const bool unicode = StringStats::CanContainUnicode(*column_stats);
		const bool min_known = StringStats::MinStringLength(*column_stats).IsValid();
		// byte length, or character length on ASCII-only data, names a real row; unicode
		// character length is only a bound (codepoints <= bytes, >= ceil(bytes/4))
		const bool attained = (kind == LengthFunctionKind::BYTE_LENGTH || !unicode) && (!is_min || min_known);
		if (partition.filter_result != FilterPropagateResult::FILTER_ALWAYS_TRUE ||
		    !stats.partition_row_group->MinMaxIsExact(storage_index) || !attained) {
			return FoldPartitionState::BOUND;
		}
		return FoldPartitionState::EXACT_VALUE;
	}

	void CombineCandidate(Value &candidate, Value &value) const {
		if (is_min ? value < candidate : value > candidate) {
			candidate = std::move(value);
		}
	}

	bool ExcludesCandidate(const Value &bound, const Value &candidate) const {
		// length bounds stay comparable even when string value stats are truncated
		return is_min ? bound >= candidate : bound <= candidate;
	}

	Value FallbackValue() const {
		// MIN/MAX over no non-null values is NULL
		return Value(result_type);
	}

private:
	bool TryGetLengthValue(BaseStatistics &column_stats, Value &value) const {
		auto expr_copy = function->Copy();
		auto &func = expr_copy->Cast<BoundFunctionExpression>();
		vector<BaseStatistics> child_stats;
		child_stats.push_back(column_stats.Copy());
		FunctionStatisticsInput input(func, func.BindInfo().get(), child_stats, &expr_copy);
		auto length_stats = func.Function().GetStatisticsCallback()(context, input);
		if (!length_stats || length_stats->GetStatsType() != StatisticsType::NUMERIC_STATS ||
		    !NumericStats::HasMinMax(*length_stats)) {
			return false;
		}
		value = is_min ? NumericStats::Min(*length_stats) : NumericStats::Max(*length_stats);
		return true;
	}

	ClientContext &context;
	unique_ptr<Expression> function;
	StorageIndex storage_index;
	bool is_min;
	LengthFunctionKind kind;
	LogicalType result_type;
};

//! MIN/MAX(f(col)) from numeric min/max via ArgProperties. Exact partitions fold; inexact or
//! filter-cut partitions only vote as BOUND. Both mapped endpoints must be usable.
struct MonotoneFoldClient {
	MonotoneFoldClient(ClientContext &context_p, MonotoneColumnInfo info_p, StorageIndex storage_index_p,
	                   LogicalType result_type_p)
	    : context(context_p), info(std::move(info_p)), storage_index(std::move(storage_index_p)),
	      result_type(std::move(result_type_p)) {
	}

	FoldPartitionState ClassifyPartition(const FoldPartition &partition, Value &value) const {
		auto &stats = partition.stats;
		if (!stats.partition_row_group) {
			return FoldPartitionState::NO_INFO;
		}
		auto column_stats = stats.partition_row_group->GetColumnStatistics(storage_index);
		if (!column_stats) {
			return FoldPartitionState::NO_INFO;
		}
		if (stats.partition_row_group->HasPendingWrites()) {
			// rows appended to this partition locally are not covered by the statistics
			return FoldPartitionState::NO_INFO;
		}
		if (column_stats->GetStatsType() != StatisticsType::NUMERIC_STATS) {
			return FoldPartitionState::NO_INFO;
		}
		const bool min_max_exact = stats.partition_row_group->MinMaxIsExact(storage_index);
		if (!NumericStats::HasMinMax(*column_stats)) {
			if (!min_max_exact) {
				return FoldPartitionState::NO_INFO;
			}
			// a partition without min/max holds no non-null values: MIN/MAX ignores them
			return column_stats->CanHaveNoNull() ? FoldPartitionState::NO_INFO : FoldPartitionState::NEUTRAL;
		}
		auto &func = info.function->Cast<BoundFunctionExpression>();
		Value out_lo, out_hi;
		if (!StatisticsPropagator::TryEvaluateMonotoneEndpoints(context, func, info.arg_values, info.column_arg,
		                                                        info.decreasing, NumericStats::Min(*column_stats),
		                                                        NumericStats::Max(*column_stats), out_lo, out_hi)) {
			return FoldPartitionState::NO_INFO;
		}
		value = info.is_min ? std::move(out_lo) : std::move(out_hi);
		// inexact/filter-cut: mapped endpoint is a bound, not attained
		if (!min_max_exact || partition.filter_result != FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			return FoldPartitionState::BOUND;
		}
		return FoldPartitionState::EXACT_VALUE;
	}

	void CombineCandidate(Value &candidate, Value &value) const {
		if (info.is_min ? value < candidate : value > candidate) {
			candidate = std::move(value);
		}
	}

	bool ExcludesCandidate(const Value &bound, const Value &candidate) const {
		// the mapped bound is numeric, so it is safe to compare.
		return info.is_min ? bound >= candidate : bound <= candidate;
	}

	Value FallbackValue() const {
		// MIN/MAX over no non-null values is NULL
		return Value(result_type);
	}

	ClientContext &context;
	MonotoneColumnInfo info;
	StorageIndex storage_index;
	LogicalType result_type;
};

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
	// MIN/MAX over a length function, e.g. MAX(strlen(col)) or MAX(length(col))
	vector<LengthColumnInfo> length_columns;
	// MIN/MAX over a monotone function of a column, e.g. MAX(year(ts))
	vector<MonotoneColumnInfo> monotone_columns;

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
			const bool is_min = fun_name == "min";
			if (aggr_expr.GetChildren().size() != 1) {
				return;
			}
			MinMaxColumnInfo column_info;
			if (!TryGetMinMaxColumnInfo(*aggr_expr.GetChildren()[0], column_info)) {
				LengthColumnInfo length_info;
				if (TryGetLengthColumnRef(*aggr_expr.GetChildren()[0], length_info)) {
					length_info.is_min = is_min;
					length_info.aggr_idx = i;
					length_columns.push_back(std::move(length_info));
					continue;
				}
				MonotoneColumnInfo monotone_info;
				if (TryGetMonotoneColumnInfo(context, *aggr_expr.GetChildren()[0], monotone_info)) {
					monotone_info.is_min = is_min;
					monotone_info.aggr_idx = i;
					monotone_columns.push_back(std::move(monotone_info));
					continue;
				}
				return;
			}
			column_info.is_min = is_min;
			column_info.aggr_idx = i;
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
		auto &proj = child_ref.get().Cast<LogicalProjection>();
		// chase colrefs; a projection may compute the length function itself (CSE)
		for (auto &column_info : length_columns) {
			auto &expr = proj.GetExpression(column_info.binding);
			if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				column_info.binding = expr.Cast<BoundColumnRefExpression>().Binding();
				continue;
			}
			if (!TryGetLengthColumnRef(expr, column_info)) {
				return;
			}
		}
		// monotone entries only chase colrefs; bail if the projection computes the argument
		for (auto &column_info : monotone_columns) {
			auto &expr = proj.GetExpression(column_info.binding);
			if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				return;
			}
			column_info.binding = expr.Cast<BoundColumnRefExpression>().Binding();
		}
		// walk backwards: an entry whose projection computes a length function moves into
		// length_columns, so it is only resolved against the next projection level
		for (idx_t i = min_max_columns.size(); i > 0; i--) {
			auto &column_info = min_max_columns[i - 1];
			auto &expr = proj.GetExpression(column_info.binding);
			MinMaxColumnInfo projection_info;
			if (!TryGetMinMaxColumnInfo(expr, projection_info)) {
				// the projection computes the value the aggregate consumes, e.g. a strlen(col) shared
				// with another aggregate and lifted out of the aggregates by CSE, or a monotone
				// function of a column
				if (column_info.result_type != expr.GetReturnType()) {
					// the aggregate casts the projected value - not a plain mapped aggregate
					return;
				}
				LengthColumnInfo length_info;
				MonotoneColumnInfo monotone_info;
				if (TryGetLengthColumnRef(expr, length_info)) {
					length_info.is_min = column_info.is_min;
					length_info.aggr_idx = column_info.aggr_idx;
					length_columns.push_back(std::move(length_info));
				} else if (TryGetMonotoneColumnInfo(context, expr, monotone_info)) {
					monotone_info.is_min = column_info.is_min;
					monotone_info.aggr_idx = column_info.aggr_idx;
					monotone_columns.push_back(std::move(monotone_info));
				} else {
					return;
				}
				min_max_columns.erase(min_max_columns.begin() + NumericCast<int64_t>(i - 1));
				comparators.erase(comparators.begin() + NumericCast<int64_t>(i - 1));
				continue;
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

	vector<StorageIndex> length_storage_indexes(length_columns.size());
	for (idx_t i = 0; i < length_columns.size(); i++) {
		auto &binding = length_columns[i].binding;
		auto &column_index = get.GetColumnIndex(binding);
		if (!get.TryGetStorageIndex(column_index, length_storage_indexes[i])) {
			return;
		}
	}

	vector<StorageIndex> monotone_storage_indexes(monotone_columns.size());
	for (idx_t i = 0; i < monotone_columns.size(); i++) {
		auto &binding = monotone_columns[i].binding;
		auto &column_index = get.GetColumnIndex(binding);
		if (!get.TryGetStorageIndex(column_index, monotone_storage_indexes[i])) {
			return;
		}
	}

	// Build the partition list shared by filter classification and folding; `original_index` is the
	// position of the partition in the row-group list
	vector<FoldPartition> partitions;
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
				partitions.emplace_back(std::move(stats), partition_idx, FilterPropagateResult::FILTER_ALWAYS_TRUE);
				break;
			case FilterPropagateResult::FILTER_ALWAYS_FALSE:
				break;
			default:
				// the filter cuts the partition: the surviving rows are a subset of the rows the
				// statistics describe, so the partition enters the fold as a bound
				partitions.emplace_back(std::move(stats), partition_idx, filter_result);
				break;
			}
		}
	} else {
		for (idx_t partition_idx = 0; partition_idx < partition_stats.size(); partition_idx++) {
			partitions.emplace_back(std::move(partition_stats[partition_idx]), partition_idx,
			                        FilterPropagateResult::FILTER_ALWAYS_TRUE);
		}
	}
	// An empty list after filter classification means every partition was filtered out entirely:
	// the aggregates run over an empty surviving set and fold to their fallback values. A
	// partition-statistics list that is empty to begin with is different - it means the statistics
	// are unknown, not that there are no rows - and is rejected earlier.

	// Fold each recognized aggregate with a stack-allocated client of its own type
	vector<Value> results(aggr.expressions.size());
	for (idx_t i = 0; i < min_max_columns.size(); i++) {
		MinMaxFoldClient client(min_max_columns[i], std::move(comparators[i]), min_max_storage_indexes[i]);
		if (!PartitionFold(partitions, client, results[min_max_columns[i].aggr_idx])) {
			// some aggregate cannot be answered from the statistics - keep the aggregate plan
			return;
		}
	}
	for (idx_t i = 0; i < length_columns.size(); i++) {
		const auto aggr_idx = length_columns[i].aggr_idx;
		LengthFoldClient client(context, std::move(length_columns[i].function), length_storage_indexes[i],
		                        length_columns[i].is_min, length_columns[i].kind,
		                        aggr.expressions[aggr_idx]->GetReturnType());
		if (!PartitionFold(partitions, client, results[aggr_idx])) {
			return;
		}
	}
	for (idx_t i = 0; i < monotone_columns.size(); i++) {
		const auto aggr_idx = monotone_columns[i].aggr_idx;
		MonotoneFoldClient client(context, std::move(monotone_columns[i]), monotone_storage_indexes[i],
		                          aggr.expressions[aggr_idx]->GetReturnType());
		if (!PartitionFold(partitions, client, results[aggr_idx])) {
			return;
		}
	}
	for (auto count_star_idx : count_star_idxs) {
		CountStarFoldClient client;
		if (!PartitionFold(partitions, client, results[count_star_idx])) {
			return;
		}
	}

	// every aggregate folded: replace it with its constant, in aggregate order
	vector<LogicalType> types(aggr.expressions.size());
	vector<unique_ptr<Expression>> agg_results(aggr.expressions.size());
	for (idx_t expr_idx = 0; expr_idx < agg_results.size(); expr_idx++) {
		auto constant = make_uniq<BoundConstantExpression>(results[expr_idx]);
		constant->SetAlias(aggr.expressions[expr_idx]->GetAlias());
		agg_results[expr_idx] = std::move(constant);
		types[expr_idx] = results[expr_idx].type();
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
