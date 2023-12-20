#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ReplaceSemiAntiBindings(vector<ColumnBinding> &bindings, Filter &filter, Expression &expr,
                                    LogicalJoin &join) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		auto join_table_index = join.GetTableIndex();
		D_ASSERT(std::count(join_table_index.begin(), join_table_index.end(), colref.binding.table_index) != 0);
		D_ASSERT(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		colref.binding = bindings[colref.binding.column_index];
		filter.bindings.insert(colref.binding.table_index);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceSemiAntiBindings(bindings, filter, child, join); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSemiAntiJoin(unique_ptr<LogicalOperator> op,
                                                                 unordered_set<idx_t> &left_bindings,
                                                                 unordered_set<idx_t> &right_bindings) {
	auto &join = op->Cast<LogicalJoin>();
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
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

	for (idx_t i = 0; i < filters.size(); i++) {
		auto side = JoinSide::GetJoinSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side
			// we can push the filter into the left side
			if (isComparison) {
				// we MIGHT be able to push it down the RHS as well, but only if it is a comparison that matches the
				// join predicates we use the FilterCombiner to figure this out add the expression to the FilterCombiner
				filter_combiner.AddFilter(filters[i]->filter->Copy());
			}
			left_pushdown.filters.push_back(std::move(filters[i]));
			// erase the filter from the list of filters
			filters.erase(filters.begin() + i);
			i--;
		}
		// TODO: These filters can be pushed into the right hand side as well.
	}

	// finally we check the FilterCombiner to see if there are any predicates we can push into the RHS
	// we only added (1) predicates that have JoinSide::BOTH from the conditions, and
	// (2) predicates that have JoinSide::LEFT from the filters
	// we check now if this combination generated any new filters that are only on JoinSide::RIGHT
	// this happens if, e.g. a join condition is (i=a) and there is a filter (i=500), we can then push the filter
	// (a=500) into the RHS
	filter_combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		if (JoinSide::GetJoinSide(*filter, left_bindings, right_bindings) == JoinSide::RIGHT) {
			right_pushdown.AddFilter(std::move(filter));
		}
	});
	right_pushdown.GenerateFilters();
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
	return PushFinalFilters(std::move(op));
}

} // namespace duckdb
