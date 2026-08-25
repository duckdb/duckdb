#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {

static Value TryGetStringStatsMin(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::VARCHAR) {
		return StringStats::TryGetValidMin(stats);
	}
	if (stats.GetType().id() == LogicalTypeId::BLOB && StringStats::GetMinType(stats) == StringStatsType::EXACT_STATS) {
		return Value::BLOB_RAW(StringStats::Min(stats));
	}
	return Value();
}

static Value TryGetStringStatsMax(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::VARCHAR) {
		return StringStats::TryGetValidMax(stats);
	}
	if (stats.GetType().id() == LogicalTypeId::BLOB && StringStats::GetMaxType(stats) == StringStatsType::EXACT_STATS) {
		return Value::BLOB_RAW(StringStats::Max(stats));
	}
	return Value();
}

static FilterPropagateResult PropagateValueComparison(const Value &lmin, const Value &lmax, const Value &rmin,
                                                      const Value &rmax, ExpressionType comparison, bool has_null) {
	const auto always_true =
	    has_null ? FilterPropagateResult::FILTER_TRUE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_TRUE;
	const auto always_false =
	    has_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		if ((!lmin.IsNull() && !rmax.IsNull() && lmin > rmax) || (!rmin.IsNull() && !lmax.IsNull() && rmin > lmax)) {
			return always_false;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHAN:
		if (!lmin.IsNull() && !rmax.IsNull() && lmin > rmax) {
			return always_true;
		}
		if (!rmin.IsNull() && !lmax.IsNull() && rmin >= lmax) {
			return always_false;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		if (!lmin.IsNull() && !rmax.IsNull() && lmin >= rmax) {
			return always_true;
		}
		if (!rmin.IsNull() && !lmax.IsNull() && rmin > lmax) {
			return always_false;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_LESSTHAN:
		if (!lmax.IsNull() && !rmin.IsNull() && lmax < rmin) {
			return always_true;
		}
		if (!lmin.IsNull() && !rmax.IsNull() && lmin >= rmax) {
			return always_false;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (!lmax.IsNull() && !rmin.IsNull() && lmax <= rmin) {
			return always_true;
		}
		if (!lmin.IsNull() && !rmax.IsNull() && lmin > rmax) {
			return always_false;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

FilterPropagateResult StatisticsPropagator::PropagateComparison(const BaseStatistics &lstats,
                                                                const BaseStatistics &rstats,
                                                                ExpressionType comparison) {
	if (lstats.GetStatsType() == StatisticsType::STRING_STATS &&
	    rstats.GetStatsType() == StatisticsType::STRING_STATS) {
		if (lstats.GetType() != rstats.GetType()) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		Value lmin;
		Value lmax;
		Value rmin;
		Value rmax;
		if (StringStats::HasMin(lstats)) {
			lmin = TryGetStringStatsMin(lstats);
		}
		if (StringStats::HasMax(lstats)) {
			lmax = TryGetStringStatsMax(lstats);
		}
		if (StringStats::HasMin(rstats)) {
			rmin = TryGetStringStatsMin(rstats);
		}
		if (StringStats::HasMax(rstats)) {
			rmax = TryGetStringStatsMax(rstats);
		}
		bool has_null = lstats.CanHaveNull() || rstats.CanHaveNull();
		return PropagateValueComparison(lmin, lmax, rmin, rmax, comparison, has_null);
	}

	switch (lstats.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		break;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!NumericStats::HasMinMax(lstats) || !NumericStats::HasMinMax(rstats)) {
		// no stats available: nothing to prune
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	bool has_null = lstats.CanHaveNull() || rstats.CanHaveNull();
	return PropagateValueComparison(NumericStats::Min(lstats), NumericStats::Max(lstats), NumericStats::Min(rstats),
	                                NumericStats::Max(rstats), comparison, has_null);
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateComparison(BoundFunctionExpression &expr,
                                                                     unique_ptr<Expression> &expr_ptr) {
	auto &left = BoundComparisonExpression::LeftMutable(expr);
	auto &right = BoundComparisonExpression::RightMutable(expr);
	auto left_stats = PropagateExpression(left);
	auto right_stats = PropagateExpression(right);
	if (!left_stats || !right_stats) {
		return nullptr;
	}
	// propagate the statistics of the comparison operator
	auto propagate_result = PropagateComparison(*left_stats, *right_stats, expr.GetExpressionType());
	switch (propagate_result) {
	case FilterPropagateResult::FILTER_ALWAYS_TRUE:
		expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		return PropagateExpression(expr_ptr);
	case FilterPropagateResult::FILTER_ALWAYS_FALSE:
		expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
		return PropagateExpression(expr_ptr);
	case FilterPropagateResult::FILTER_TRUE_OR_NULL: {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(left));
		children.push_back(std::move(right));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(true));
		return nullptr;
	}
	case FilterPropagateResult::FILTER_FALSE_OR_NULL: {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(left));
		children.push_back(std::move(right));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(false));
		return nullptr;
	}
	default:
		// FIXME: we can propagate nulls here, i.e. this expression will have nulls only if left and right has nulls
		return nullptr;
	}
}

} // namespace duckdb
