#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

bool StatisticsPropagator::ExpressionIsConstant(Expression &expr, Value val) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &bound_constant = (BoundConstantExpression&) expr;
	D_ASSERT(bound_constant.value.type() == val.type());
	return bound_constant.value == val;
}

bool StatisticsPropagator::ExpressionIsConstantOrNull(Expression &expr, Value val) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &bound_function = (BoundFunctionExpression&) expr;
	return ConstantOrNull::IsConstantOrNull(bound_function, val);
}

void StatisticsPropagator::UpdateFilterStatistics(BaseStatistics &stats, ExpressionType comparison_type, Value constant) {
	// any comparison filter removes all null values
	stats.has_null = false;
	if (!stats.type.IsNumeric()) {
		// don't handle non-numeric columns here (yet)
		return;
	}
	auto &numeric_stats = (NumericStatistics &)stats;
	if (numeric_stats.min.is_null || numeric_stats.max.is_null) {
		// no stats available: skip this
		return;
	}
	switch(comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// X < constant OR X <= constant
		// max becomes the constant
		numeric_stats.max = constant;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// X > constant OR X >= constant
		// min becomes the constant
		numeric_stats.min = constant;
		break;
	case ExpressionType::COMPARE_EQUAL:
		// X = constant
		// both min and max become the constant
		numeric_stats.min = constant;
		numeric_stats.max = constant;
		break;
	default:
		break;
	}
}

void StatisticsPropagator::SetStatisticsNotNull(ColumnBinding binding) {
	auto entry = statistics_map.find(binding);
	if (entry == statistics_map.end()) {
		return;
	}
	entry->second->has_null = false;
}

void StatisticsPropagator::UpdateFilterStatistics(Expression &condition) {
	// in filters, we check for constant comparisons with bound columns
	// if we find a comparison in the form of e.g. "i=3", we can update our statistics for that column
	if (condition.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = (BoundComparisonExpression &) condition;
		// first check if either side is a bound column ref
		if (comparison.left->type == ExpressionType::BOUND_COLUMN_REF) {
			SetStatisticsNotNull(((BoundColumnRefExpression&) *comparison.left).binding);
		}
		if (comparison.right->type == ExpressionType::BOUND_COLUMN_REF) {
			SetStatisticsNotNull(((BoundColumnRefExpression&) *comparison.right).binding);
		}
		// check if this is a comparison between a constant and a column ref
		BoundConstantExpression *constant;
		BoundColumnRefExpression *columnref;
		ExpressionType comparison_type = comparison.type;
		if (comparison.left->type == ExpressionType::VALUE_CONSTANT &&
			comparison.right->type == ExpressionType::BOUND_COLUMN_REF) {
			constant = (BoundConstantExpression*) comparison.left.get();
			columnref = (BoundColumnRefExpression*) comparison.right.get();
		} else if (
			comparison.left->type == ExpressionType::BOUND_COLUMN_REF &&
			comparison.right->type == ExpressionType::VALUE_CONSTANT) {
			columnref = (BoundColumnRefExpression*) comparison.left.get();
			constant = (BoundConstantExpression*) comparison.right.get();
			comparison_type = FlipComparisionExpression(comparison_type);
		} else {
			// unsupported filter: check if either side is a bound comparison
			return;
		}
		auto entry = statistics_map.find(columnref->binding);
		if (entry == statistics_map.end()) {
			return;
		}
		UpdateFilterStatistics(*entry->second, comparison_type, constant->value);
	}
}

void StatisticsPropagator::PropagateStatistics(LogicalFilter &filter, unique_ptr<LogicalOperator> *node_ptr) {
	// first propagate to the child
	PropagateStatistics(filter.children[0]);

	// then propagate to each of the expressions
	for(idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &condition = filter.expressions[i];
		PropagateExpression(condition);
	}

	for(idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &condition = *filter.expressions[i];
		if (ExpressionIsConstant(condition, Value::BOOLEAN(true))) {
			// filter is always true; it is useless to execute it
			// erase this condition
			filter.expressions.erase(filter.expressions.begin() + i);
			i--;
		} else if (ExpressionIsConstant(condition, Value::BOOLEAN(false)) ||
			ExpressionIsConstantOrNull(condition, Value::BOOLEAN(false))) {
			// filter is always false or null; this entire filter should be replaced by an empty result block
			ReplaceWithEmptyResult(*node_ptr);
			return;
		}
		// cannot prune this filter: propagate statistics from the filter
		UpdateFilterStatistics(condition);
	}
}

}
