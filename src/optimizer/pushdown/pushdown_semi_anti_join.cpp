#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ReplaceSemiAntiBindings(vector<ColumnBinding> bindings, Filter &filter, Expression &expr,
                                    LogicalJoin &join) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto left_bindings = join.children[0]->GetColumnBindings();
		auto right_bindings = join.children[1]->GetColumnBindings();
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		vector<idx_t> table_indexes;

		D_ASSERT(colref.depth == 0);

		if (std::find(bindings.begin(), bindings.end(), colref.binding) != bindings.end()) {
			// filter is already applied to the correct bindings.
			return;
		}

		// if the condition is not distinct from or equality, then you can push on both sides

		// colref binding can't be found in the given bindings, which means it was created
		// for other side of the join.
		if (std::find(left_bindings.begin(), left_bindings.end(), colref.binding) != left_bindings.end()) {
			// the original filter came from the left side, push it down on the right
			colref.binding = right_bindings[colref.binding.column_index];
			filter.bindings.insert(colref.binding.table_index);
		} else if (std::find(right_bindings.begin(), right_bindings.end(), colref.binding) != right_bindings.end()) {
			// the original filter came from the right side, push it down the left
			colref.binding = left_bindings[colref.binding.column_index];
			filter.bindings.insert(colref.binding.table_index);
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceSemiAntiBindings(bindings, filter, child, join); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSemiAntiJoin(unique_ptr<LogicalOperator> op,
                                                                 unordered_set<idx_t> &left_bindings,
                                                                 unordered_set<idx_t> &right_bindings) {
	auto &join = op->Cast<LogicalJoin>();
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		return FinishPushdown(std::move(op));
	}

	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	// for a comparison join we create a FilterCombiner that checks if we can push conditions on LHS join conditions
	// into the RHS of the join
	FilterCombiner filter_combiner(optimizer);
	const auto isComparison = (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	                           op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN);
	if (isComparison) {
		// add all comparison conditions
		auto &comparison_join = op->Cast<LogicalComparisonJoin>();
		for (auto &cond : comparison_join.conditions) {
			filter_combiner.AddFilter(
			    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy()));
		}
	}
	auto left_bindings_actual = op->children[0]->GetColumnBindings();
	auto right_bindings_actual = op->children[1]->GetColumnBindings();
	D_ASSERT(left_bindings_actual.size() == right_bindings_actual.size());

	// take every filter, and attempt to push it down the left and the right side.
	for (idx_t i = 0; i < filters.size(); i++) {
		// first create a copy of the filter
		auto right_filter = make_uniq<Filter>();
		auto left_filter = make_uniq<Filter>();
		right_filter->filter = filters[i]->filter->Copy();
		left_filter->filter = filters[i]->filter->Copy();

		// in the original filter, rewrite references to the result of the union into references to the left_index
		ReplaceSemiAntiBindings(op->children[0]->GetColumnBindings(), *left_filter, *left_filter->filter, join);
		// in the copied filter, rewrite references to the result of the union into references to the right_index
		ReplaceSemiAntiBindings(op->children[1]->GetColumnBindings(), *right_filter, *right_filter->filter, join);

		// extract bindings again
		left_filter->ExtractBindings();
		right_filter->ExtractBindings();

		// move the filters into the child pushdown nodes
		left_pushdown.filters.push_back(std::move(left_filter));
		right_pushdown.filters.push_back(std::move(right_filter));
	}

	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));

	bool left_empty = op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	bool right_empty = op->children[1]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	if (left_empty && right_empty) {
		// both empty: return empty result
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	// filter pushdown happens before join order optimization, so left_anti and left_semi are not possible yet here
	if (left_empty) {
		// left child is empty result
		switch (join.join_type) {
		case JoinType::ANTI:
		case JoinType::SEMI:
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			break;
		}
	} else if (right_empty) {
		// right child is empty result
		switch (join.join_type) {
		case JoinType::ANTI:
			// just return the left child.
			return std::move(op->children[0]);
		case JoinType::SEMI:
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			break;
		}
	}
	return std::move(op);
}

} // namespace duckdb
