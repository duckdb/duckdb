#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundBetweenExpression &between,
                                                                     unique_ptr<Expression> &expr_ptr) {
	// propagate in all the children
	auto input_stats = PropagateExpression(between.InputMutable());
	auto lower_stats = PropagateExpression(between.LowerBoundMutable());
	auto upper_stats = PropagateExpression(between.UpperBoundMutable());
	if (!input_stats) {
		return nullptr;
	}
	auto lower_comparison = between.LowerComparisonType();
	auto upper_comparison = between.UpperComparisonType();
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
		children.push_back(std::move(between.InputMutable()));
		children.push_back(std::move(between.LowerBoundMutable()));
		children.push_back(std::move(between.UpperBoundMutable()));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(false));
	} else if (lower_prune == FilterPropagateResult::FILTER_TRUE_OR_NULL &&
	           upper_prune == FilterPropagateResult::FILTER_TRUE_OR_NULL) {
		// both filters are true or null: replace with a true or null
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(between.InputMutable()));
		children.push_back(std::move(between.LowerBoundMutable()));
		children.push_back(std::move(between.UpperBoundMutable()));
		expr_ptr = ExpressionRewriter::ConstantOrNull(std::move(children), Value::BOOLEAN(true));
	} else if (lower_prune == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
		// lower filter is always true: replace with upper comparison
		expr_ptr = make_uniq<BoundComparisonExpression>(upper_comparison, std::move(between.InputMutable()),
		                                                std::move(between.UpperBoundMutable()));
	} else if (upper_prune == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
		// upper filter is always true: replace with lower comparison
		expr_ptr = make_uniq<BoundComparisonExpression>(lower_comparison, std::move(between.InputMutable()),
		                                                std::move(between.LowerBoundMutable()));
	}
	return nullptr;
}

} // namespace duckdb
