#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//! Whether the filter compares what a column of that child is compared as with constants: it keeps or removes all the
//! rows that the set operation treats as equal
static bool ComparesSetOperationColumn(const Expression &filter, const vector<JoinCondition> &comparisons,
                                       bool left_child) {
	auto compared = FilterCombiner::GetComparedExpression(filter);
	if (!compared) {
		return false;
	}
	for (auto &comparison : comparisons) {
		if ((left_child ? comparison.GetLHS() : comparison.GetRHS()).Equals(*compared)) {
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> FilterPullup::PullupSetOperation(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_EXCEPT);
	if (!context) {
		return FinishPullup(std::move(op));
	}
	auto &setop = op->Cast<LogicalSetOperation>();
	// the children cannot get more columns to carry a filter
	FilterPullup left_pullup(context, true, false);
	FilterPullup right_pullup(context, op->type == LogicalOperatorType::LOGICAL_INTERSECT, false);
	op->children[0] = left_pullup.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(std::move(op->children[1]));
	auto comparisons = setop.GetComparisons(*context);

	// EXCEPT and INTERSECT return some of the rows of the left child that they treat as equal. A filter of the left
	// child may run above them if it keeps or removes all equal rows: one that compares a column with constants.
	// A filter that stays below and can throw keeps the filters that guard it there
	auto &left_filters = left_pullup.filters_expr_pullup;
	bool keep_all = false;
	for (auto &filter : left_filters) {
		keep_all = keep_all || (filter->CanThrow() && !ComparesSetOperationColumn(*filter, comparisons, true));
	}
	vector<unique_ptr<Expression>> kept_filters;
	vector<unique_ptr<Expression>> pulled_filters;
	for (auto &filter : left_filters) {
		if (keep_all || !ComparesSetOperationColumn(*filter, comparisons, true)) {
			kept_filters.push_back(std::move(filter));
		} else {
			pulled_filters.push_back(std::move(filter));
		}
	}
	left_filters.clear();
	if (!kept_filters.empty()) {
		op->children[0] = GeneratePullupFilter(std::move(op->children[0]), kept_filters);
	}
	if (!right_pullup.filters_expr_pullup.empty()) {
		// INTERSECT: a left row with an equal right row passes what the filters of the right child imply for it
		vector<reference<const Expression>> right_filters;
		for (auto &filter : right_pullup.filters_expr_pullup) {
			right_filters.push_back(*filter);
		}
		FilterCombiner::GenerateJoinFilters(
		    comparisons, right_filters, JoinSide::LEFT,
		    [&](unique_ptr<Expression> left_filter) { pulled_filters.push_back(std::move(left_filter)); });
		op->children[1] = GeneratePullupFilter(std::move(op->children[1]), right_pullup.filters_expr_pullup);
	}
	if (pulled_filters.empty()) {
		return op;
	}
	// the filters refer to the columns of the left child: above the set operation these are its columns
	auto left_bindings = op->children[0]->GetColumnBindings();
	for (auto &filter : pulled_filters) {
		filter = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
		    std::move(filter), [&](const BoundColumnRefExpression &colref) {
			    D_ASSERT(colref.Depth() == 0);
			    for (idx_t i = 0; i < left_bindings.size(); i++) {
				    if (left_bindings[i] == colref.Binding()) {
					    return colref.WithBinding(ColumnBinding(setop.table_index, ProjectionIndex(i)));
				    }
			    }
			    throw InternalException("Filter of a set operation refers to a column that is not one of its child");
		    });
	}
	return GeneratePullupFilter(std::move(op), pulled_filters);
}

} // namespace duckdb
