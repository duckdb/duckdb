#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateBetween(BoundFunctionExpression &between,
                                                                  unique_ptr<Expression> &expr_ptr) {
	// propagate in all the children
	auto &input = BoundBetweenExpression::InputMutable(between);
	auto &lower_bound = BoundBetweenExpression::LowerBoundMutable(between);
	auto &upper_bound = BoundBetweenExpression::UpperBoundMutable(between);
	auto input_stats = PropagateExpression(input);
	auto lower_stats = PropagateExpression(lower_bound);
	auto upper_stats = PropagateExpression(upper_bound);
	if (!input_stats) {
		return nullptr;
	}
	auto lower_comparison = BoundBetweenExpression::LowerComparisonType(between);
	auto upper_comparison = BoundBetweenExpression::UpperComparisonType(between);
	// propagate the comparisons
	auto lower_prune = FilterPropagateResult::NO_PRUNING_POSSIBLE;
	auto upper_prune = FilterPropagateResult::NO_PRUNING_POSSIBLE;
	if (lower_stats) {
		lower_prune = PropagateComparison(*input_stats, *lower_stats, lower_comparison);
	}
	if (upper_stats) {
		upper_prune = PropagateComparison(*input_stats, *upper_stats, upper_comparison);
	}
	if (lower_prune == FilterPropagateResult::FILTER_ALWAYS_TRUE &&
	    upper_prune == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
		// both filters are always true: replace the between expression with a constant true
		expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	} else if (lower_prune == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
	           upper_prune == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
		// either one of the filters is always false: replace the between expression with a constant false
		expr_ptr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
	} else if (lower_prune == FilterPropagateResult::FILTER_FALSE_OR_NULL ||
	           upper_prune == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
		// either one of the filters is false or null: replace with a constant or null (false)
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(input));
		children.push_back(std::move(lower_bound));
		children.push_back(std::move(upper_bound));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(false));
	} else if (lower_prune == FilterPropagateResult::FILTER_TRUE_OR_NULL &&
	           upper_prune == FilterPropagateResult::FILTER_TRUE_OR_NULL) {
		// both filters are true or null: replace with a true or null
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(input));
		children.push_back(std::move(lower_bound));
		children.push_back(std::move(upper_bound));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(true));
	} else if (lower_prune == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
		// lower filter is always true: replace with upper comparison
		expr_ptr = make_uniq<BoundComparisonExpression>(upper_comparison, std::move(input), std::move(upper_bound));
	} else if (upper_prune == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
		// upper filter is always true: replace with lower comparison
		expr_ptr = make_uniq<BoundComparisonExpression>(lower_comparison, std::move(input), std::move(lower_bound));
	}
	return nullptr;
}

} // namespace duckdb
