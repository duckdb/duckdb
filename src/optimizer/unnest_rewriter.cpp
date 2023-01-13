#include "duckdb/optimizer/unnest_rewriter.hpp"

#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

void UnnestRewriter::FindCandidates(unique_ptr<LogicalOperator> *op_ptr,
                                    vector<unique_ptr<LogicalOperator> *> &candidates) {
	auto op = op_ptr->get();
	// search children before adding, so the deepest candidates get added first
	for (auto &child : op->children) {
		FindCandidates(&child, candidates);
	}

	// search for operator that has a LOGICAL_DELIM_JOIN as its child
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

void UnnestRewriter::UpdateColumnBindings(idx_t &old_table_index, idx_t &new_table_index,
                                          unique_ptr<Expression> &expr) {

	// these expression classes do not have children and need no update, return
	if (expr->expression_class == ExpressionClass::BOUND_CONSTANT ||
	    expr->expression_class == ExpressionClass::BOUND_PARAMETER ||
	    expr->expression_class == ExpressionClass::BOUND_LAMBDA_REF) {
		return;
	}

	// these expression classes do not have children, transform them
	if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {

		auto &bound_column_ref = (BoundColumnRefExpression &)*expr;
		if (bound_column_ref.binding.table_index == old_table_index) {
			bound_column_ref.binding.table_index = new_table_index;
		}

	} else {
		// recursively enumerate the children of the expression
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			UpdateColumnBindings(old_table_index, new_table_index, child);
		});
	}
}

void UnnestRewriter::RewriteCandidate(unique_ptr<LogicalOperator> *candidate) {

	auto &projection = (LogicalProjection &)**candidate;
	// the LOGICAL_DELIM_JOIN is a child of the projection and has two children
	auto &delim_join = *projection.children[0];

	// lhs of the LOGICAL_DELIM_JOIN is a LOGICAL_WINDOW that contains a LOGICAL_PROJECTION
	// this lhs_proj later becomes the child of the UNNEST
	auto &window = *delim_join.children[0];
	auto &lhs_proj = window.children[0];

	// rhs of the LOGICAL_DELIM_JOIN is a LOGICAL_PROJECTION that contains the UNNEST
	// remember the table index of rhs_proj
	auto proj_table_index = ((LogicalProjection &)*delim_join.children[1]).table_index;
	auto &rhs_proj = *delim_join.children[1];

	// update the plan by making the lhs_proj the child of the UNNEST
	// and by making the rhs_proj the child of the topmost LOGICAL_PROJECTION
	// thereby replacing the LOGICAL_DELIM_JOIN
	auto &unnest = (LogicalUnnest &)*rhs_proj.children[0];
	unnest.children[0] = std::move(lhs_proj);
	projection.children[0] = std::move(rhs_proj.children[0]);

	// update the column binding of the BOUND_UNNEST expression of the LOGICAL_UNNEST
	auto &bound_unnest_expr = (BoundUnnestExpression &)*unnest.expressions[0];
	auto &bound_column_ref_expr = (BoundColumnRefExpression &)*bound_unnest_expr.child;
	auto &unnest_child_op = (LogicalProjection &)*unnest.children[0];
	bound_column_ref_expr.binding.table_index = unnest_child_op.table_index;
	bound_column_ref_expr.binding.column_index = 0;

	// recursively visit expressions and replace proj_table_index with the unnest index
	for (auto &expr : projection.expressions) {
		UpdateColumnBindings(proj_table_index, unnest.unnest_index, expr);
	}
}

unique_ptr<LogicalOperator> UnnestRewriter::Optimize(unique_ptr<LogicalOperator> op) {

	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	// transform candidates
	for (auto &candidate : candidates) {
		RewriteCandidate(candidate);
	}

	return op;
}

} // namespace duckdb
