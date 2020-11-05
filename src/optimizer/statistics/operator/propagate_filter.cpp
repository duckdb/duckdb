#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

FilterPropagateResult StatisticsPropagator::PropagateFilter(ColumnBinding binding, ExpressionType comparison_type, Value constant) {
	// find the statistics for the column, if any
	auto entry = statistics_map.find(binding);
	if (entry == statistics_map.end()) {
		// no statistics to update
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	// any comparison removes all null values from the column
	D_ASSERT(entry->second.get());
	auto &stats = *entry->second;
	stats.has_null = false;
	if (!stats.type.IsIntegral()) {
		// this is all we do for non-numeric stats (for now?)
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &numeric_stats = (NumericStatistics &)stats;
	if (numeric_stats.min.is_null || numeric_stats.max.is_null) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	switch(comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
		// X < constant
		// if min(x) >= constant, this filter always evaluates to false
		if (numeric_stats.min >= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		// if max(x) < constant, this filter always evaluates to true
		if (numeric_stats.max < constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if not, max becomes the constant
		numeric_stats.max = constant;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// X <= constant
		// if min(x) > constant, this filter always evaluates to false
		if (numeric_stats.min > constant) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		// if max(x) <= constant, this filter always evaluates to true
		if (numeric_stats.max <= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if not, max becomes the constant
		numeric_stats.max = constant;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		// X > constant
		// if max(x) <= constant, this filter always evaluates to false
		if (numeric_stats.max <= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		// if min(x) > constant, this filter always evaluates to true
		if (numeric_stats.min > constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if not, min becomes the constant
		numeric_stats.min = constant;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// X >= constant
		// if max(x) < constant, this filter always evaluates to false
		if (numeric_stats.max < constant) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		// if min(x) >= constant, this filter always evaluates to true
		if (numeric_stats.min >= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// if not, min becomes the constant
		numeric_stats.min = constant;
		break;
	case ExpressionType::COMPARE_EQUAL:
		// X = constant
		// if min(x) > constant, or max(x) < constant, this filter always evalutes to false
		if (numeric_stats.min > constant) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (numeric_stats.max < constant) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		// if min = max, the filter always evalutes to true
		if (numeric_stats.min == numeric_stats.max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		// otherwise, both min and max become the constant
		numeric_stats.min = constant;
		numeric_stats.max = constant;
		break;
	default:
		break;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

FilterPropagateResult StatisticsPropagator::PropagateFilter(Expression &condition) {
	// in filters, we check for constant comparisons with bound columns
	// if we find a comparison in the form of e.g. "i=3", we can update our statistics for that column
	if (condition.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = (BoundComparisonExpression &) condition;
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
			// unsupported filter
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return PropagateFilter(columnref->binding, comparison_type, constant->value);
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

bool StatisticsPropagator::PropagateStatistics(LogicalFilter &filter) {
	// first propagate to the child
	if (PropagateStatistics(*filter.children[0])) {
		// child needs to be removed: also remove this node
		return true;
	}
	// then propagate to each of the expressions
	for(idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &condition = filter.expressions[i];
		PropagateExpression(*condition);
	}

	for(idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &condition = filter.expressions[i];
		auto propagate_result = PropagateFilter(*condition);
		if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			// filter is always true; it is useless to execute it
			// erase this condition
			filter.expressions.erase(filter.expressions.begin() + i);
			i--;
		} else if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			// filter is always false; this entire filter should be replaced by an empty result block
			return true;
		}
	}
	return false;
}

}
