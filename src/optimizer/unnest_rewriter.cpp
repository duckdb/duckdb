#include "duckdb/optimizer/unnest_rewriter.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
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

	if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &bound_column_ref = expr->Cast<BoundColumnRefExpression>();
		for (idx_t i = 0; i < replace_bindings.size(); i++) {
			if (bound_column_ref.binding == replace_bindings[i].old_binding) {
				bound_column_ref.binding = replace_bindings[i].new_binding;
				break;
			}
		}
	}

	VisitExpressionChildren(**expression);
}

unique_ptr<LogicalOperator> UnnestRewriter::Optimize(unique_ptr<LogicalOperator> op) {

	UnnestRewriterPlanUpdater updater;
	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	// rewrite the plan and update the bindings
	for (auto &candidate : candidates) {

		// rearrange the logical operators
		if (RewriteCandidate(candidate)) {
			updater.overwritten_tbl_idx = overwritten_tbl_idx;
			// update the bindings of the BOUND_UNNEST expression
			UpdateBoundUnnestBindings(updater, candidate);
			// update the sequence of LOGICAL_PROJECTION(s)
			UpdateRHSBindings(&op, candidate, updater);
			// reset
			delim_columns.clear();
			lhs_bindings.clear();
		}
	}

	return op;
}

void UnnestRewriter::FindCandidates(unique_ptr<LogicalOperator> *op_ptr,
                                    vector<unique_ptr<LogicalOperator> *> &candidates) {
	auto op = op_ptr->get();
	// search children before adding, so that we add candidates bottom-up
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

	// found a delim join
	auto &delim_join = op->children[0]->Cast<LogicalComparisonJoin>();
	// only support INNER delim joins
	if (delim_join.join_type != JoinType::INNER) {
		return;
	}
	// INNER delim join must have exactly one condition
	if (delim_join.conditions.size() != 1) {
		return;
	}

	// LHS child is a window
	idx_t delim_idx = delim_join.delim_flipped ? 1 : 0;
	idx_t other_idx = 1 - delim_idx;
	if (delim_join.children[delim_idx]->type != LogicalOperatorType::LOGICAL_WINDOW) {
		return;
	}

	// RHS child must be projection(s) followed by an UNNEST
	auto curr_op = &delim_join.children[other_idx];
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if (curr_op->get()->children.size() != 1) {
			break;
		}
		curr_op = &curr_op->get()->children[0];
	}

	if (curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST) {
		candidates.push_back(op_ptr);
	}
}

bool UnnestRewriter::RewriteCandidate(unique_ptr<LogicalOperator> *candidate) {

	auto &topmost_op = (LogicalOperator &)**candidate;
	if (topmost_op.type != LogicalOperatorType::LOGICAL_PROJECTION &&
	    topmost_op.type != LogicalOperatorType::LOGICAL_WINDOW &&
	    topmost_op.type != LogicalOperatorType::LOGICAL_FILTER &&
	    topmost_op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
	    topmost_op.type != LogicalOperatorType::LOGICAL_UNNEST) {
		return false;
	}

	// get the LOGICAL_DELIM_JOIN, which is a child of the candidate
	D_ASSERT(topmost_op.children.size() == 1);
	auto &delim_join = topmost_op.children[0]->Cast<LogicalComparisonJoin>();
	D_ASSERT(delim_join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	GetDelimColumns(delim_join);

	// LHS of the LOGICAL_DELIM_JOIN is a LOGICAL_WINDOW that contains a LOGICAL_PROJECTION
	// this lhs_proj later becomes the child of the UNNEST

	idx_t delim_idx = delim_join.delim_flipped ? 1 : 0;
	idx_t other_idx = 1 - delim_idx;
	auto &window = *delim_join.children[delim_idx];
	auto &lhs_op = window.children[0];
	GetLHSExpressions(*lhs_op);

	// find the LOGICAL_UNNEST
	// and get the path down to the LOGICAL_UNNEST
	vector<unique_ptr<LogicalOperator> *> path_to_unnest;
	auto curr_op = &delim_join.children[other_idx];
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		path_to_unnest.push_back(curr_op);
		curr_op = &curr_op->get()->children[0];
	}

	// store the table index of the child of the LOGICAL_UNNEST
	// then update the plan by making the lhs_proj the child of the LOGICAL_UNNEST
	D_ASSERT(curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST);
	auto &unnest = curr_op->get()->Cast<LogicalUnnest>();
	D_ASSERT(unnest.children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET);
	overwritten_tbl_idx = unnest.children[0]->Cast<LogicalDelimGet>().table_index;

	D_ASSERT(!unnest.children.empty());
	auto &delim_get = unnest.children[0]->Cast<LogicalDelimGet>();
	D_ASSERT(delim_get.chunk_types.size() > 1);
	distinct_unnest_count = delim_get.chunk_types.size();
	unnest.children[0] = std::move(lhs_op);

	// replace the LOGICAL_DELIM_JOIN with its RHS child operator
	topmost_op.children[0] = std::move(*path_to_unnest.front());
	return true;
}

void UnnestRewriter::UpdateRHSBindings(unique_ptr<LogicalOperator> *plan_ptr, unique_ptr<LogicalOperator> *candidate,
                                       UnnestRewriterPlanUpdater &updater) {

	auto &topmost_op = (LogicalOperator &)**candidate;
	idx_t shift = lhs_bindings.size();

	vector<unique_ptr<LogicalOperator> *> path_to_unnest;
	auto curr_op = &topmost_op.children[0];
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {

		path_to_unnest.push_back(curr_op);
		D_ASSERT(curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION);
		auto &proj = curr_op->get()->Cast<LogicalProjection>();

		// pop the unnest columns and the delim index
		D_ASSERT(proj.expressions.size() > distinct_unnest_count);
		for (idx_t i = 0; i < distinct_unnest_count; i++) {
			proj.expressions.pop_back();
		}

		// store all shifted current bindings
		idx_t tbl_idx = proj.table_index;
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			ReplaceBinding replace_binding(ColumnBinding(tbl_idx, i), ColumnBinding(tbl_idx, i + shift));
			updater.replace_bindings.push_back(replace_binding);
		}

		curr_op = &curr_op->get()->children[0];
	}

	// update all bindings by shifting them
	updater.VisitOperator(*plan_ptr->get());
	updater.replace_bindings.clear();

	// update all bindings coming from the LHS to RHS bindings
	D_ASSERT(topmost_op.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto &top_proj = topmost_op.children[0]->Cast<LogicalProjection>();
	for (idx_t i = 0; i < lhs_bindings.size(); i++) {
		ReplaceBinding replace_binding(lhs_bindings[i].binding, ColumnBinding(top_proj.table_index, i));
		updater.replace_bindings.push_back(replace_binding);
	}

	// temporarily remove the BOUND_UNNESTs and the child of the LOGICAL_UNNEST from the plan
	D_ASSERT(curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST);
	auto &unnest = curr_op->get()->Cast<LogicalUnnest>();
	vector<unique_ptr<Expression>> temp_bound_unnests;
	for (auto &temp_bound_unnest : unnest.expressions) {
		temp_bound_unnests.push_back(std::move(temp_bound_unnest));
	}
	D_ASSERT(unnest.children.size() == 1);
	auto temp_unnest_child = std::move(unnest.children[0]);
	unnest.expressions.clear();
	unnest.children.clear();
	// update the bindings of the plan
	updater.VisitOperator(*plan_ptr->get());
	updater.replace_bindings.clear();
	// add the children again
	for (auto &temp_bound_unnest : temp_bound_unnests) {
		unnest.expressions.push_back(std::move(temp_bound_unnest));
	}
	unnest.children.push_back(std::move(temp_unnest_child));

	// add the LHS expressions to each LOGICAL_PROJECTION
	for (idx_t i = path_to_unnest.size(); i > 0; i--) {

		D_ASSERT(path_to_unnest[i - 1]->get()->type == LogicalOperatorType::LOGICAL_PROJECTION);
		auto &proj = path_to_unnest[i - 1]->get()->Cast<LogicalProjection>();

		// temporarily store the existing expressions
		vector<unique_ptr<Expression>> existing_expressions;
		for (idx_t expr_idx = 0; expr_idx < proj.expressions.size(); expr_idx++) {
			existing_expressions.push_back(std::move(proj.expressions[expr_idx]));
		}

		proj.expressions.clear();

		// add the new expressions
		for (idx_t expr_idx = 0; expr_idx < lhs_bindings.size(); expr_idx++) {
			auto new_expr = make_uniq<BoundColumnRefExpression>(
			    lhs_bindings[expr_idx].alias, lhs_bindings[expr_idx].type, lhs_bindings[expr_idx].binding);
			proj.expressions.push_back(std::move(new_expr));

			// update the table index
			lhs_bindings[expr_idx].binding.table_index = proj.table_index;
			lhs_bindings[expr_idx].binding.column_index = expr_idx;
		}

		// add the existing expressions again
		for (idx_t expr_idx = 0; expr_idx < existing_expressions.size(); expr_idx++) {
			proj.expressions.push_back(std::move(existing_expressions[expr_idx]));
		}
	}
}

void UnnestRewriter::UpdateBoundUnnestBindings(UnnestRewriterPlanUpdater &updater,
                                               unique_ptr<LogicalOperator> *candidate) {

	auto &topmost_op = (LogicalOperator &)**candidate;

	// traverse LOGICAL_PROJECTION(s)
	auto curr_op = &topmost_op.children[0];
	while (curr_op->get()->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		curr_op = &curr_op->get()->children[0];
	}

	// found the LOGICAL_UNNEST
	D_ASSERT(curr_op->get()->type == LogicalOperatorType::LOGICAL_UNNEST);
	auto &unnest = curr_op->get()->Cast<LogicalUnnest>();

	D_ASSERT(unnest.children.size() == 1);
	auto unnest_cols = unnest.children[0]->GetColumnBindings();

	for (idx_t i = 0; i < delim_columns.size(); i++) {
		auto delim_binding = delim_columns[i];

		auto unnest_it = unnest_cols.begin();
		while (unnest_it != unnest_cols.end()) {
			auto unnest_binding = *unnest_it;

			if (delim_binding.table_index == unnest_binding.table_index) {
				unnest_binding.table_index = overwritten_tbl_idx;
				unnest_binding.column_index++;
				updater.replace_bindings.emplace_back(unnest_binding, delim_binding);
				unnest_cols.erase(unnest_it);
				break;
			}
			unnest_it++;
		}
	}

	// update bindings
	for (auto &unnest_expr : unnest.expressions) {
		updater.VisitExpression(&unnest_expr);
	}
	updater.replace_bindings.clear();
}

void UnnestRewriter::GetDelimColumns(LogicalOperator &op) {

	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &delim_join = op.Cast<LogicalComparisonJoin>();
	for (idx_t i = 0; i < delim_join.duplicate_eliminated_columns.size(); i++) {
		auto &expr = *delim_join.duplicate_eliminated_columns[i];
		D_ASSERT(expr.type == ExpressionType::BOUND_COLUMN_REF);
		auto &bound_colref_expr = expr.Cast<BoundColumnRefExpression>();
		delim_columns.push_back(bound_colref_expr.binding);
	}
}

void UnnestRewriter::GetLHSExpressions(LogicalOperator &op) {

	op.ResolveOperatorTypes();
	auto col_bindings = op.GetColumnBindings();
	D_ASSERT(op.types.size() == col_bindings.size());

	bool set_alias = false;
	// we can easily extract the alias for LOGICAL_PROJECTION(s)
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op.Cast<LogicalProjection>();
		if (proj.expressions.size() == op.types.size()) {
			set_alias = true;
		}
	}

	for (idx_t i = 0; i < op.types.size(); i++) {
		lhs_bindings.emplace_back(col_bindings[i], op.types[i]);
		if (set_alias) {
			auto &proj = op.Cast<LogicalProjection>();
			lhs_bindings.back().alias = proj.expressions[i]->alias;
		}
	}
}

} // namespace duckdb
