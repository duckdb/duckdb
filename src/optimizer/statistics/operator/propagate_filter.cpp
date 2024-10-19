#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

static bool IsCompareDistinct(ExpressionType type) {
	return type == ExpressionType::COMPARE_DISTINCT_FROM || type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

bool StatisticsPropagator::ExpressionIsConstant(Expression &expr, const Value &val) {
	Value expr_value;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		expr_value = expr.Cast<BoundConstantExpression>().value;
	} else if (expr.IsFoldable()) {
		if (!ExpressionExecutor::TryEvaluateScalar(context, expr, expr_value)) {
			return false;
		}
	} else {
		return false;
	}
	D_ASSERT(expr_value.type() == val.type());
	return Value::NotDistinctFrom(expr_value, val);
}

bool StatisticsPropagator::ExpressionIsConstantOrNull(Expression &expr, const Value &val) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &bound_function = expr.Cast<BoundFunctionExpression>();
	return ConstantOrNull::IsConstantOrNull(bound_function, val);
}

void StatisticsPropagator::SetStatisticsNotNull(ColumnBinding binding) {
	auto entry = statistics_map.find(binding);
	if (entry == statistics_map.end()) {
		return;
	}
	entry->second->Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
}

void StatisticsPropagator::UpdateFilterStatistics(BaseStatistics &stats, ExpressionType comparison_type,
                                                  const Value &constant) {
	// regular comparisons removes all null values
	if (!IsCompareDistinct(comparison_type)) {
		stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	}
	if (!stats.GetType().IsNumeric()) {
		// don't handle non-numeric columns here (yet)
		return;
	}
	if (!NumericStats::HasMinMax(stats)) {
		// no stats available: skip this
		return;
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// X < constant OR X <= constant
		// max becomes the constant
		NumericStats::SetMax(stats, constant);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// X > constant OR X >= constant
		// min becomes the constant
		NumericStats::SetMin(stats, constant);
		break;
	case ExpressionType::COMPARE_EQUAL:
		// X = constant
		// both min and max become the constant
		NumericStats::SetMin(stats, constant);
		NumericStats::SetMax(stats, constant);
		break;
	default:
		break;
	}
}

void StatisticsPropagator::UpdateFilterStatistics(BaseStatistics &lstats, BaseStatistics &rstats,
                                                  ExpressionType comparison_type) {
	// regular comparisons removes all null values
	if (!IsCompareDistinct(comparison_type)) {
		lstats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		rstats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	}
	D_ASSERT(lstats.GetType() == rstats.GetType());
	if (!lstats.GetType().IsNumeric()) {
		// don't handle non-numeric columns here (yet)
		return;
	}
	if (!NumericStats::HasMinMax(lstats) || !NumericStats::HasMinMax(rstats)) {
		// no stats available: skip this
		return;
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// LEFT < RIGHT OR LEFT <= RIGHT
		// we know that every value of left is smaller (or equal to) every value in right
		// i.e. if we have left = [-50, 250] and right = [-100, 100]

		// we know that left.max is AT MOST equal to right.max
		// because any value in left that is BIGGER than right.max will not pass the filter
		if (NumericStats::Max(lstats) > NumericStats::Max(rstats)) {
			NumericStats::SetMax(lstats, NumericStats::Max(rstats));
		}

		// we also know that right.min is AT MOST equal to left.min
		// because any value in right that is SMALLER than left.min will not pass the filter
		if (NumericStats::Min(rstats) < NumericStats::Min(lstats)) {
			NumericStats::SetMin(rstats, NumericStats::Min(lstats));
		}
		// so in our example, the bounds get updated as follows:
		// left: [-50, 100], right: [-50, 100]
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// LEFT > RIGHT OR LEFT >= RIGHT
		// we know that every value of left is bigger (or equal to) every value in right
		// this is essentially the inverse of the less than (or equal to) scenario
		if (NumericStats::Max(rstats) > NumericStats::Max(lstats)) {
			NumericStats::SetMax(rstats, NumericStats::Max(lstats));
		}
		if (NumericStats::Min(lstats) < NumericStats::Min(rstats)) {
			NumericStats::SetMin(lstats, NumericStats::Min(rstats));
		}
		break;
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		// LEFT = RIGHT
		// only the tightest bounds pass
		// so if we have e.g. left = [-50, 250] and right = [-100, 100]
		// the tighest bounds are [-50, 100]
		// select the highest min
		if (NumericStats::Min(lstats) > NumericStats::Min(rstats)) {
			NumericStats::SetMin(rstats, NumericStats::Min(lstats));
		} else {
			NumericStats::SetMin(lstats, NumericStats::Min(rstats));
		}
		// select the lowest max
		if (NumericStats::Max(lstats) < NumericStats::Max(rstats)) {
			NumericStats::SetMax(rstats, NumericStats::Max(lstats));
		} else {
			NumericStats::SetMax(lstats, NumericStats::Max(rstats));
		}
		break;
	default:
		break;
	}
}

void StatisticsPropagator::UpdateFilterStatistics(Expression &left, Expression &right, ExpressionType comparison_type) {
	// first check if either side is a bound column ref
	// any column ref involved in a comparison will not be null after the comparison
	bool compare_distinct = IsCompareDistinct(comparison_type);
	if (!compare_distinct && left.type == ExpressionType::BOUND_COLUMN_REF) {
		SetStatisticsNotNull((left.Cast<BoundColumnRefExpression>()).binding);
	}
	if (!compare_distinct && right.type == ExpressionType::BOUND_COLUMN_REF) {
		SetStatisticsNotNull((right.Cast<BoundColumnRefExpression>()).binding);
	}
	// check if this is a comparison between a constant and a column ref
	optional_ptr<BoundConstantExpression> constant;
	optional_ptr<BoundColumnRefExpression> columnref;
	if (left.type == ExpressionType::VALUE_CONSTANT && right.type == ExpressionType::BOUND_COLUMN_REF) {
		constant = &left.Cast<BoundConstantExpression>();
		columnref = &right.Cast<BoundColumnRefExpression>();
		comparison_type = FlipComparisonExpression(comparison_type);
	} else if (left.type == ExpressionType::BOUND_COLUMN_REF && right.type == ExpressionType::VALUE_CONSTANT) {
		columnref = &left.Cast<BoundColumnRefExpression>();
		constant = &right.Cast<BoundConstantExpression>();
	} else if (left.type == ExpressionType::BOUND_COLUMN_REF && right.type == ExpressionType::BOUND_COLUMN_REF) {
		// comparison between two column refs
		auto &left_column_ref = left.Cast<BoundColumnRefExpression>();
		auto &right_column_ref = right.Cast<BoundColumnRefExpression>();
		auto lentry = statistics_map.find(left_column_ref.binding);
		auto rentry = statistics_map.find(right_column_ref.binding);
		if (lentry == statistics_map.end() || rentry == statistics_map.end()) {
			return;
		}
		UpdateFilterStatistics(*lentry->second, *rentry->second, comparison_type);
	} else {
		// unsupported filter
		return;
	}
	if (constant && columnref) {
		// comparison between columnref
		auto entry = statistics_map.find(columnref->binding);
		if (entry == statistics_map.end()) {
			return;
		}
		UpdateFilterStatistics(*entry->second, comparison_type, constant->value);
	}
}

void StatisticsPropagator::UpdateFilterStatistics(Expression &condition) {
	// in filters, we check for constant comparisons with bound columns
	// if we find a comparison in the form of e.g. "i=3", we can update our statistics for that column
	switch (condition.GetExpressionClass()) {
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between = condition.Cast<BoundBetweenExpression>();
		UpdateFilterStatistics(*between.input, *between.lower, between.LowerComparisonType());
		UpdateFilterStatistics(*between.input, *between.upper, between.UpperComparisonType());
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comparison = condition.Cast<BoundComparisonExpression>();
		UpdateFilterStatistics(*comparison.left, *comparison.right, comparison.type);
		break;
	}
	default:
		break;
	}
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalFilter &filter,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	// first propagate to the child
	node_stats = PropagateStatistics(filter.children[0]);
	if (filter.children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		ReplaceWithEmptyResult(node_ptr);
		return make_uniq<NodeStatistics>(0U, 0U);
	}

	// then propagate to each of the expressions
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &condition = filter.expressions[i];
		PropagateExpression(condition);

		if (ExpressionIsConstant(*condition, Value::BOOLEAN(true))) {
			// filter is always true; it is useless to execute it
			// erase this condition
			filter.expressions.erase_at(i);
			i--;
			if (filter.expressions.empty()) {
				// if there is a projection map, we should keep the filter
				// the physical planner will eventually skip the filter, but will keep
				// the correct columns.
				if (filter.projection_map.empty()) {
					node_ptr = std::move(filter.children[0]);
				}
				break;
			}
		} else if (ExpressionIsConstant(*condition, Value::BOOLEAN(false)) ||
		           ExpressionIsConstantOrNull(*condition, Value::BOOLEAN(false))) {
			// filter is always false or null; this entire filter should be replaced by an empty result block
			ReplaceWithEmptyResult(node_ptr);
			return make_uniq<NodeStatistics>(0U, 0U);
		} else {
			// cannot prune this filter: propagate statistics from the filter
			UpdateFilterStatistics(*condition);
		}
	}
	// the max cardinality of a filter is the cardinality of the input (i.e. no tuples get filtered)
	return std::move(node_stats);
}

} // namespace duckdb
