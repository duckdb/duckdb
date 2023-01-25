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
		for (idx_t i = 0; i < replace_bindings.size(); i++) {
			if (bound_column_ref.binding == replace_bindings[i].old_binding) {
				bound_column_ref.binding = replace_bindings[i].new_binding;
			}
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

	// found a delim join
	auto &delim_join = (LogicalDelimJoin &)*op->children[0];

	// TODO: also support joins with more than the default condition?
	// delim join must have exactly one condition
	if (delim_join.conditions.size() != 1) {
		return;
	}

	// TODO: also support other operators on lhs?
	// lhs child is a window that contains a projection
	if (delim_join.children[0]->type != LogicalOperatorType::LOGICAL_WINDOW) {
		return;
	}
	if (delim_join.children[0]->children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// rhs child must be projection(s) followed by an UNNEST
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

void UnnestRewriter::UpdateRHSBindings(unique_ptr<LogicalOperator> *plan_ptr, unique_ptr<LogicalOperator> *candidate,
                                       UnnestRewriterPlanUpdater &updater) {

	auto &topmost_op = (LogicalOperator &)**candidate;
	idx_t shift = lhs_expressions.size();
	updater.replace_bindings.clear();

	vector<unique_ptr<LogicalOperator> *> path_to_unnest;
	auto curr_op = &(topmost_op.children[0]);
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {

		path_to_unnest.push_back(curr_op);
		auto &proj = (LogicalProjection &)*curr_op->get();

		// remove the delim_index BOUND_COLUMN_REF by iterating the expressions
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			if (proj.expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &bound_colref_expr = (BoundColumnRefExpression &)*proj.expressions[i];
				if (bound_colref_expr.alias == "delim_index") {
					proj.expressions.erase(proj.expressions.begin() + i);
					break;
				}
			}
		}

		// store all shifted current bindings
		idx_t tbl_idx = proj.table_index;
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			ReplaceBinding replace_binding(ColumnBinding(tbl_idx, i), ColumnBinding(tbl_idx, i + shift));
			updater.replace_bindings.push_back(replace_binding);
		}

		curr_op = &curr_op->get()->children[0];
	}

	// make sure that the parent LOGICAL_PROJECTION of the LOGICAL_UNNEST
	// only contains one expression
	auto &back = (LogicalProjection &)*(*path_to_unnest.back());
	back.expressions.pop_back();
	updater.replace_bindings.pop_back();

	// update all bindings by shifting them
	updater.VisitOperator(*plan_ptr->get());

	// update all bindings coming from the LHS to RHS bindings
	auto &top_proj = (LogicalProjection &)*topmost_op.children[0];
	updater.replace_bindings.clear();
	for (idx_t i = 0; i < lhs_expressions.size(); i++) {
		ReplaceBinding replace_binding(ColumnBinding(lhs_tbl_idx, i), ColumnBinding(top_proj.table_index, i));
		updater.replace_bindings.push_back(replace_binding);
	}
	// temporarily remove the BOUND_UNNEST from the plan
	auto &unnest = (LogicalUnnest &)*curr_op->get();
	auto temp_bound_unnest = std::move(unnest.expressions[0]);
	unnest.expressions.clear();
	// update the bindings of the plan
	updater.VisitOperator(*plan_ptr->get());
	// add the child again
	unnest.expressions.push_back(std::move(temp_bound_unnest));

	// add the expressions to each LOGICAL_PROJECTION
	auto curr_tbl_idx = lhs_tbl_idx;
	for (idx_t i = path_to_unnest.size(); i > 0; i--) {

		auto &proj = (LogicalProjection &)*path_to_unnest[i - 1]->get();

		// temporarily store the existing expressions
		vector<unique_ptr<Expression>> existing_expressions;
		for (idx_t expr_idx = 0; expr_idx < proj.expressions.size(); expr_idx++) {
			existing_expressions.push_back(std::move(proj.expressions[expr_idx]));
		}

		proj.expressions.clear();

		// add the new expressions
		for (idx_t expr_idx = 0; expr_idx < lhs_expressions.size(); expr_idx++) {
			auto new_expr = lhs_expressions[expr_idx]->Copy();
			auto &bound_col_ref = (BoundColumnRefExpression &)*new_expr;
			bound_col_ref.binding.table_index = curr_tbl_idx;
			bound_col_ref.binding.column_index = expr_idx;
			proj.expressions.push_back(std::move(new_expr));
		}

		// add the existing expressions again
		for (idx_t expr_idx = 0; expr_idx < existing_expressions.size(); expr_idx++) {
			proj.expressions.push_back(std::move(existing_expressions[expr_idx]));
		}

		curr_tbl_idx = proj.table_index;
	}
}

void UnnestRewriter::GetLHSExpressions(LogicalOperator &op) {

	auto &proj = (LogicalProjection &)op;
	lhs_tbl_idx = proj.table_index;

	for (idx_t i = 0; i < proj.expressions.size(); i++) {
		auto return_type = proj.expressions[i]->return_type;
		auto alias = proj.expressions[i]->alias;
		auto expr = make_unique<BoundColumnRefExpression>(alias, return_type, ColumnBinding());
		lhs_expressions.push_back(std::move(expr));
	}
}

void UnnestRewriter::GetDelimColumns(LogicalOperator &op) {

	auto &delim_join = (LogicalDelimJoin &)op;
	for (idx_t i = 0; i < delim_join.duplicate_eliminated_columns.size(); i++) {
		auto &expr = *delim_join.duplicate_eliminated_columns[i];
		auto &bound_colref_expr = (BoundColumnRefExpression &)expr;
		delim_columns.push_back(bound_colref_expr.binding);
	}
}

bool UnnestRewriter::RewriteCandidate(unique_ptr<LogicalOperator> *candidate) {

	auto &topmost_op = (LogicalOperator &)**candidate;
	if (topmost_op.type != LogicalOperatorType::LOGICAL_PROJECTION &&
	    topmost_op.type != LogicalOperatorType::LOGICAL_WINDOW) {
		return false;
		// TODO: add topmost_op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY
		//		throw InternalException("Error in UnnestRewriter: unknown parent for LOGICAL_DELIM_JOIN: \"%s\"",
		//				                        LogicalOperatorToString((*candidate)->type));
	}

	// get the LOGICAL_DELIM_JOIN, which is a child of the candidate
	// TODO: can the candidate have more than one LOGICAL_DELIM_JOIN as its children?
	D_ASSERT(topmost_op.children.size() == 1);
	auto &delim_join = *(topmost_op.children[0]);
	D_ASSERT(delim_join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	GetDelimColumns(delim_join);

	// lhs of the LOGICAL_DELIM_JOIN is a LOGICAL_WINDOW that contains a LOGICAL_PROJECTION
	// this lhs_proj later becomes the child of the UNNEST
	auto &window = *delim_join.children[0];
	auto &lhs_proj = window.children[0];
	GetLHSExpressions(*lhs_proj);

	// find the LOGICAL_UNNEST
	// and get the path down to the LOGICAL_UNNEST
	vector<unique_ptr<LogicalOperator> *> path_to_unnest;
	auto curr_op = &(delim_join.children[1]);
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		path_to_unnest.push_back(curr_op);
		curr_op = &curr_op->get()->children[0];
	}

	// store the table index of the child of the LOGICAL_UNNEST
	// then update the plan by making the lhs_proj the child of the LOGICAL_UNNEST
	D_ASSERT(curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST);
	auto &unnest = (LogicalUnnest &)*curr_op->get();
	unnest_idx = unnest.unnest_index;
	overwritten_tbl_idx = ((LogicalProjection &)*unnest.children[0]).table_index;
	unnest.children[0] = std::move(lhs_proj);

	// replace the LOGICAL_DELIM_JOIN with its rhs child operator
	topmost_op.children[0] = std::move(*path_to_unnest.front());
	return true;
}

void UnnestRewriter::UpdateBoundUnnestBinding(unique_ptr<LogicalOperator> *candidate) {

	auto &topmost_op = (LogicalOperator &)**candidate;

	// traverse LOGICAL_PROJECTION(s)
	auto curr_op = &(topmost_op.children[0]);
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		curr_op = &curr_op->get()->children[0];
	}

	// found the LOGICAL_UNNEST
	auto &unnest = (LogicalUnnest &)*curr_op->get();

	// update the column binding of the BOUND_UNNEST expression of the LOGICAL_UNNEST
	auto &bound_unnest_expr = (BoundUnnestExpression &)*unnest.expressions[0];
	auto &bound_column_ref_expr = (BoundColumnRefExpression &)*bound_unnest_expr.child;
	auto &unnest_child_op = (LogicalProjection &)*unnest.children[0];
	bound_column_ref_expr.binding.table_index = unnest_child_op.table_index;
	for (idx_t i = 0; i < delim_columns.size(); i++) {
		if (delim_columns[i].table_index == unnest_child_op.table_index) {
			bound_column_ref_expr.binding.column_index = delim_columns[i].column_index;
			break;
		}
	}
}

unique_ptr<LogicalOperator> UnnestRewriter::Optimize(unique_ptr<LogicalOperator> op) {

	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	// rewrite the plan and update the bindings
	for (auto &candidate : candidates) {
		// rearrange the logical operators
		// TODO: change, just temporarily
		auto did_rewrite = RewriteCandidate(candidate);

		if (did_rewrite) {
			// update the binding of the BOUND_UNNEST expression
			UpdateBoundUnnestBinding(candidate);

			// update the sequence of LOGICAL_PROJECTION(s)
			UnnestRewriterPlanUpdater updater;
			UpdateRHSBindings(&op, candidate, updater);
		}

		delim_columns.clear();
		lhs_expressions.clear();
	}

	return op;
}

} // namespace duckdb
