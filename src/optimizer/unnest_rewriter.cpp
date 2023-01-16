#include "duckdb/optimizer/unnest_rewriter.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

void UnnestRewriterPlanUpdater::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void UnnestRewriterPlanUpdater::VisitExpression(unique_ptr<Expression> *expression) {

	auto &expr = *expression;

	// these expression classes do not have children, transform them
	if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

		auto &bound_column_ref = (BoundColumnRefExpression &)*expr;
		if (bound_column_ref.binding.table_index == old_table_idx) {
			D_ASSERT(bound_column_ref.binding.column_index == 0);
			bound_column_ref.binding.table_index = new_table_idx;
		}
	}

	VisitExpressionChildren(**expression);
}

void UnnestRewriter::FindCandidates(unique_ptr<LogicalOperator> *op_ptr,
                                    vector<unique_ptr<LogicalOperator> *> &candidates) {
	auto op = op_ptr->get();
	// search children before adding, so that we add candidates bottom-up
	for (auto &child : op->children) {
		FindCandidates(&child, candidates);
	}

	// search for operator that has a LOGICAL_DELIM_JOIN as its child
	// TODO: can there be more then one child? With possibly multiple delim joins as children?
	if (op->children.size() != 1) {
		return;
	}
	if (op->children[0]->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return;
	}

	// found a delim join, rhs child must be projection(s) followed by an UNNEST
	auto &delim_join = *op->children[0];
	auto curr_op = &delim_join.children[1];
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (curr_op->get()->children.size() != 1) {
			break;
		}
		curr_op = &curr_op->get()->children[0];
	}

	if (curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST) {
		candidates.push_back(op_ptr);
	}
	return;
}

pair<idx_t, idx_t> UnnestRewriter::RewriteCandidate(unique_ptr<LogicalOperator> *candidate) {

	auto &topmost_op = (LogicalOperator &)**candidate;
	if (topmost_op.type != LogicalOperatorType::LOGICAL_PROJECTION &&
	    topmost_op.type != LogicalOperatorType::LOGICAL_WINDOW) {
		return pair<idx_t, idx_t>(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);

		// TODO: add topmost_op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY
		//		throw InternalException("Error in UnnestRewriter: unknown parent for LOGICAL_DELIM_JOIN: \"%s\"",
		//				                        LogicalOperatorToString((*candidate)->type));
	}

	// get the LOGICAL_DELIM_JOIN, which is a child of the candidate
	// TODO: can the candidate have more than one LOGICAL_DELIM_JOIN as its children?
	D_ASSERT(topmost_op.children.size() == 1);
	auto &delim_join = *(topmost_op.children[0]);
	D_ASSERT(delim_join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN);

	// lhs of the LOGICAL_DELIM_JOIN is a LOGICAL_WINDOW that contains a LOGICAL_PROJECTION
	// this lhs_proj later becomes the child of the UNNEST
	auto &window = *delim_join.children[0];
	auto &lhs_proj = window.children[0];

	// find the LOGICAL_UNNEST
	// and get the path down to the LOGICAL_UNNEST, to later update the column bindings correctly
	vector<unique_ptr<LogicalOperator> *> path_to_unnest;
	auto curr_op = &(delim_join.children[1]);
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		D_ASSERT(curr_op->get()->children.size() == 1);
		path_to_unnest.push_back(curr_op);
		curr_op = &curr_op->get()->children[0];
	}

	// update the plan by making the lhs_proj the child of the LOGICAL_UNNEST
	D_ASSERT(curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST);
	auto &unnest = (LogicalUnnest &)*curr_op->get();
	unnest.children[0] = std::move(lhs_proj);

	// update the column binding of the BOUND_UNNEST expression of the LOGICAL_UNNEST
	auto &bound_unnest_expr = (BoundUnnestExpression &)*unnest.expressions[0];
	auto &bound_column_ref_expr = (BoundColumnRefExpression &)*bound_unnest_expr.child;
	auto &unnest_child_op = (LogicalProjection &)*unnest.children[0];
	bound_column_ref_expr.binding.table_index = unnest_child_op.table_index;
	bound_column_ref_expr.binding.column_index = 0;

	// save the table index of the LOGICAL_PROJECTION containing the LOGICAL_UNNEST
	// because it will be overwritten later
	auto &old_proj = *path_to_unnest.back()->get();
	auto old_proj_table_index = ((LogicalProjection &)old_proj).table_index;
	path_to_unnest.pop_back();

	// now update the plan by moving the LOGICAL_UNNEST one operator up,
	// overwriting the LOGICAL_PROJECTION
	if (path_to_unnest.empty()) {
		// if there are no other LOGICAL_PROJECTION(s) on the path down to the LOGICAL_UNNEST,
		// then this also replaces the LOGICAL_DELIM_JOIN with the LOGICAL_UNNEST operator
		topmost_op.children[0] = std::move(*curr_op);

	} else {
		// otherwise, there are other LOGICAL_PROJECTION(s)
		auto &parent_of_unnest = (LogicalOperator &)(*path_to_unnest.back()->get());
		parent_of_unnest.children[0] = std::move(*curr_op);

		// remove all delim_index expressions
		for (idx_t i = 0; i < path_to_unnest.size(); i++) {
			auto &curr_proj = *(path_to_unnest[i])->get();
			D_ASSERT(curr_proj.expressions.back()->type == ExpressionType::BOUND_COLUMN_REF);
			curr_proj.expressions.pop_back();
		}

		// replace the LOGICAL_DELIM_JOIN with its rhs child operator, which is not the LOGICAL_UNNEST
		topmost_op.children[0] = std::move(*path_to_unnest.front());
	}

	return pair<idx_t, idx_t>(old_proj_table_index, unnest.unnest_index);

	// TODO: still missing: add all scanned columns (lhs) not relevant to the unnest also into the other projections
}

unique_ptr<LogicalOperator> UnnestRewriter::Optimize(unique_ptr<LogicalOperator> op) {

	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	// transform candidates
	for (auto &candidate : candidates) {
		auto update_idx_pair = RewriteCandidate(candidate);
		UnnestRewriterPlanUpdater updater(update_idx_pair.first, update_idx_pair.second);
		updater.VisitOperator(*op);
	}

	return op;
}

} // namespace duckdb
