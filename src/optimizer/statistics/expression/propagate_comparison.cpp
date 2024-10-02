#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"

namespace duckdb {

FilterPropagateResult StatisticsPropagator::PropagateComparison(BaseStatistics &lstats, BaseStatistics &rstats,
                                                                ExpressionType comparison) {
	// only handle numerics for now
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
	// the result of the propagation depend on whether or not either side has null values
	// if there are null values present, we cannot say whether or not
	bool has_null = lstats.CanHaveNull() || rstats.CanHaveNull();
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		// l = r, if l.min > r.max or r.min > l.max equality is not possible
		if (NumericStats::Min(lstats) > NumericStats::Max(rstats) ||
		    NumericStats::Min(rstats) > NumericStats::Max(lstats)) {
			return has_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
		} else {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	case ExpressionType::COMPARE_GREATERTHAN:
		// l > r
		if (NumericStats::Min(lstats) > NumericStats::Max(rstats)) {
			// if l.min > r.max, it is always true ONLY if neither side contains nulls
			return has_null ? FilterPropagateResult::FILTER_TRUE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if r.min is bigger or equal to l.max, the filter is always false
		if (NumericStats::Min(rstats) >= NumericStats::Max(lstats)) {
			return has_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// l >= r
		if (NumericStats::Min(lstats) >= NumericStats::Max(rstats)) {
			// if l.min >= r.max, it is always true ONLY if neither side contains nulls
			return has_null ? FilterPropagateResult::FILTER_TRUE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if r.min > l.max, the filter is always false
		if (NumericStats::Min(rstats) > NumericStats::Max(lstats)) {
			return has_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_LESSTHAN:
		// l < r
		if (NumericStats::Max(lstats) < NumericStats::Min(rstats)) {
			// if l.max < r.min, it is always true ONLY if neither side contains nulls
			return has_null ? FilterPropagateResult::FILTER_TRUE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if l.min >= rstats.max, the filter is always false
		if (NumericStats::Min(lstats) >= NumericStats::Max(rstats)) {
			return has_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// l <= r
		if (NumericStats::Max(lstats) <= NumericStats::Min(rstats)) {
			// if l.max <= r.min, it is always true ONLY if neither side contains nulls
			return has_null ? FilterPropagateResult::FILTER_TRUE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if l.min > rstats.max, the filter is always false
		if (NumericStats::Min(lstats) > NumericStats::Max(rstats)) {
			return has_null ? FilterPropagateResult::FILTER_FALSE_OR_NULL : FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundComparisonExpression &expr,
                                                                     unique_ptr<Expression> &expr_ptr) {
	auto left_stats = PropagateExpression(expr.left);
	auto right_stats = PropagateExpression(expr.right);
	if (!left_stats || !right_stats) {
		return nullptr;
	}
	// propagate the statistics of the comparison operator
	auto propagate_result = PropagateComparison(*left_stats, *right_stats, expr.type);
	switch (propagate_result) {
	case FilterPropagateResult::FILTER_ALWAYS_TRUE:
		expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		return PropagateExpression(expr_ptr);
	case FilterPropagateResult::FILTER_ALWAYS_FALSE:
		expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
		return PropagateExpression(expr_ptr);
	case FilterPropagateResult::FILTER_TRUE_OR_NULL: {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(expr.left));
		children.push_back(std::move(expr.right));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(true));
		return nullptr;
	}
	case FilterPropagateResult::FILTER_FALSE_OR_NULL: {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(expr.left));
		children.push_back(std::move(expr.right));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(false));
		return nullptr;
	}
	default:
		// FIXME: we can propagate nulls here, i.e. this expression will have nulls only if left and right has nulls
		return nullptr;
	}
}

} // namespace duckdb
