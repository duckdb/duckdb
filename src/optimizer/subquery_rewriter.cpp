#include "optimizer/subquery_rewriter.hpp"

#include "optimizer/expression_rewriter.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

bool ExtractCorrelatedExpressions(LogicalOperator *op, LogicalOperator *current_op, SubqueryExpression *subquery, size_t subquery_table_index,
                                  vector<JoinCondition> &join_conditions);

unique_ptr<LogicalOperator> SubqueryRewriter::Rewrite(unique_ptr<LogicalOperator> plan) {
	if (plan->type == LogicalOperatorType::FILTER) {
		// rewrite subqueries within a filter
		auto &filter = (LogicalFilter &)*plan;
		// now rewrite subqueries within the filter
		for (size_t i = 0; i < filter.expressions.size(); i++) {
			auto &expr = *filter.expressions[i];
			// check if we know how to rewrite this type of subquery
			switch (expr.type) {
			case ExpressionType::COMPARE_IN:
			case ExpressionType::COMPARE_NOT_IN: {
				auto &op_expr = (OperatorExpression&) expr;
				// (NOT) IN, check if we are dealing with a subquery
				if (op_expr.children.size() != 2) {
					// only constant lists have multiple elements
					break;
				}
				if (op_expr.children[1]->type == ExpressionType::SELECT_SUBQUERY) {
					// IN (SUBQUERY), rewrite
					if (RewriteInClause(filter, (OperatorExpression *)filter.expressions[i].get(),
					                    (SubqueryExpression *)op_expr.children[1].get())) {
						// successful rewrite, remove the expression from the set of filter expressions
						filter.expressions.erase(filter.expressions.begin() + i);
						i--;
					}
				}
				break;
			}
			case ExpressionType::OPERATOR_EXISTS:
			case ExpressionType::OPERATOR_NOT_EXISTS: {
				auto &op_expr = (OperatorExpression&) expr;
				// NOT (EXISTS), check if we are dealing with a subquery
				assert(op_expr.children.size() == 1);
				if (op_expr.children[0]->type == ExpressionType::SELECT_SUBQUERY) {
					// EXISTS (SUBQUERY), rewrite
					if (RewriteExistsClause(filter, (OperatorExpression *)filter.expressions[i].get(),
					                        (SubqueryExpression *)op_expr.children[0].get())) {
						// successful rewrite, remove the expression from the set of filter expressions
						filter.expressions.erase(filter.expressions.begin() + i);
						i--;
					}
				}
				break;
			}
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOTEQUAL:
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
				auto &comp_expr = (ComparisonExpression&) expr;
				// comparison, check if we are dealing with a subquery
				if (comp_expr.left->type == ExpressionType::SELECT_SUBQUERY ||
				    comp_expr.right->type == ExpressionType::SELECT_SUBQUERY) {
					int subquery_index = comp_expr.left->type == ExpressionType::SELECT_SUBQUERY ? 0 : 1;
					auto subquery = (SubqueryExpression*) (subquery_index == 0 ? comp_expr.left.get() : comp_expr.right.get());
					// Comparison with subquery, rewrite
					if (RewriteSubqueryComparison(filter, (ComparisonExpression *)filter.expressions[i].get(),
					                              subquery)) {
						// successful rewrite, remove the expression from the set of filter expressions
						filter.expressions.erase(filter.expressions.begin() + i);
						i--;
					}
				}
				break;
			}
			default:
				break;
			}
		}
		if (filter.expressions.size() == 0) {
			// the filter is empty, return its child instead
			return Rewrite(move(filter.children[0]));
		}
	}
	// recursively rewrite the children of the current node as well
	for (size_t i = 0; i < plan->children.size(); i++) {
		plan->children[i] = Rewrite(move(plan->children[i]));
	}
	return plan;
}

bool SubqueryRewriter::RewriteInClause(LogicalFilter &filter, OperatorExpression *operator_expression,
                                       SubqueryExpression *subquery) {
	// we found a COMPARE_IN/COMPARE_NOT_IN with a subquery
	if (subquery->is_correlated) {
		// correlated subqueries not handled yet for in clause rewrite
		return false;
	}
	// find the projection
	auto node = GetProjection(subquery->op.get());

	// convert the expression into a semi-join or anti-join
	// first figure out the join type
	JoinType type;
	if (operator_expression->type == ExpressionType::COMPARE_IN) {
		type = JoinType::SEMI;
	} else { /* COMPARE_NOT_IN */
		assert(operator_expression->type == ExpressionType::COMPARE_NOT_IN);
		type = JoinType::ANTI;
	}

	// generate a table index for the new subquery
	auto subquery_table_index = context.GenerateTableIndex();

	// create the join condition
	JoinCondition condition;
	// it is between the left_expression of the IN clause
	condition.left = move(operator_expression->children[0]);
	// and the first column of the subquery
	assert(node->expressions.size() > 0);
	condition.right =
	    make_unique<ColumnRefExpression>(node->expressions[0]->return_type, ColumnBinding(subquery_table_index, 0));
	condition.comparison = ExpressionType::COMPARE_EQUAL;

	// now convert the subquery expression into a proper subquery
	auto table_subquery = make_unique<LogicalSubquery>(subquery_table_index, node->expressions.size());
	table_subquery->children.push_back(move(subquery->op));

	// create the join between the new subquery and the child of the filter
	auto join = make_unique<LogicalJoin>(type);
	join->children.push_back(move(filter.children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions.push_back(move(condition));

	// finally we remove the original in-expression from the filter
	filter.children[0] = move(join);
	return true;
}

bool SubqueryRewriter::RewriteExistsClause(LogicalFilter &filter, OperatorExpression *exists,
                                           SubqueryExpression *subquery) {
	// Only rewrite correlated exists
	// Non-correlated exists should be rewritten in a different way
	// Because either (1) the whole result is empty, or (2) it's a NOP operation
	if (!subquery->is_correlated) {
		return false;
	}

	// find the projection
	auto node = GetProjection(subquery->op.get());
	if (node->type != LogicalOperatorType::PROJECTION) {
		// only know how to rewrite normal projections in EXISTS clause
		return false;
	}

	// figure out the join type
	JoinType type;
	if (exists->type == ExpressionType::OPERATOR_EXISTS) {
		type = JoinType::SEMI;
	} else { /* OPERATOR_NOT_EXISTS */
		assert(exists->type == ExpressionType::OPERATOR_NOT_EXISTS);
		type = JoinType::ANTI;
	}

	// now we turn a subquery in the WHERE clause into a "proper" subquery
	// hence we need to get a new table index from the BindContext
	auto subquery_table_index = context.GenerateTableIndex();

	// step 2: find correlations to add to the list of join conditions
	vector<JoinCondition> join_conditions;
	if (!ExtractCorrelatedExpressions(node, node, subquery, subquery_table_index, join_conditions)) {
		return false;
	}

	// unlike equality comparison with subquery we only have the correlated
	// expressions as join condition
	if (join_conditions.size() == 0) {
		return false;
	}
	bool has_only_inequality = true;
	for (auto &condition : join_conditions) {
		if (condition.comparison != ExpressionType::COMPARE_NOTEQUAL) {
			has_only_inequality = false;
			break;
		}
	}
	if (has_only_inequality) {
		// only inequality comparisons
		// we flip them to equality comparisons and invert the join
		// this allows us to use a hash join
		for (auto &condition : join_conditions) {
			condition.comparison = ExpressionType::COMPARE_EQUAL;
		}
		if (type == JoinType::SEMI) {
			type = JoinType::ANTI;
		} else {
			type = JoinType::SEMI;
		}
	}

	// now we add join between the filter and the subquery
	assert(filter.children.size() == 1);

	auto table_subquery = make_unique<LogicalSubquery>(subquery_table_index, node->expressions.size());
	table_subquery->children.push_back(move(subquery->op));

	auto join = make_unique<LogicalJoin>(type);
	join->children.push_back(move(filter.children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions = move(join_conditions);

	filter.children[0] = move(join);

	// finally we remove the original equality expression from the filter
	return true;
}

static void AddColumnIndex(Expression *expr, size_t old_groups, size_t aggr_groups_size) {
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto colref = (ColumnRefExpression *)expr;
		if (colref->index >= old_groups) {
			colref->index += aggr_groups_size - old_groups;
		}
	}
	expr->EnumerateChildren([&](Expression *child) {
		AddColumnIndex(child, old_groups, aggr_groups_size);
	});

}

bool SubqueryRewriter::RewriteSubqueryComparison(LogicalFilter &filter, ComparisonExpression *comparison,
                                                 SubqueryExpression *subquery) {
	// rewrite a comparison with a subquery (e.g. A == (SUBQUERY))
	// step 1: check that subquery is a projection
	auto node = GetProjection(subquery->op.get());
	if (node->type != LogicalOperatorType::PROJECTION) {
		return false;
	}
	if (node->children[0]->type != LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto proj = (LogicalProjection *)node;
	auto aggr = (LogicalAggregate *)node->children[0].get();

	// now we turn a subquery in the WHERE clause into a "proper" subquery
	// hence we need to get a new table index from the BindContext
	auto subquery_table_index = context.GenerateTableIndex();

	size_t old_groups = aggr->groups.size();
	// step 2: find correlations to add to the list of join conditions
	vector<JoinCondition> join_conditions;
	if (!ExtractCorrelatedExpressions(aggr, aggr, subquery, subquery_table_index, join_conditions)) {
		return false;
	}

	// we might have added new groups
	// since groups occur BEFORE aggregates in the output of the aggregate, we need to shift column references to
	// aggregates by the amount of new groups we added
	AddColumnIndex(proj->expressions[0].get(), old_groups, aggr->groups.size());
	// for each of the new join conditions with correlated expressions we added we have to:
	// (1) project the group column in the projection
	// (2) set the right join condition so it references the group column
	for (size_t i = 0; i < join_conditions.size(); i++) {
		auto type = aggr->groups[old_groups + i]->return_type;
		// create the grouping column
		proj->expressions.push_back(make_unique<ColumnRefExpression>(type, old_groups + i));
		// now make the join condition reference this column
		join_conditions[i].right =
		    make_unique<ColumnRefExpression>(type, ColumnBinding(subquery_table_index, proj->expressions.size() - 1));
	}

	// create the join condition based on the equality expression
	// first is the original condition
	JoinCondition condition;
	condition.left =
	    subquery == comparison->left.get() ? move(comparison->right) : move(comparison->left);
	// the right condition is the aggregate of the subquery
	condition.right =
	    make_unique<ColumnRefExpression>(proj->expressions[0]->return_type, ColumnBinding(subquery_table_index, 0));
	condition.comparison = comparison->type;

	// now we add join between the filter and the subquery
	assert(filter.children.size() == 1);

	auto table_subquery = make_unique<LogicalSubquery>(subquery_table_index, proj->expressions.size());
	table_subquery->children.push_back(move(subquery->op));

	auto join = make_unique<LogicalJoin>(JoinType::INNER);
	join->children.push_back(move(filter.children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions.push_back(move(condition));
	for (auto &condition : join_conditions) {
		join->conditions.push_back(move(condition));
	}

	filter.children[0] = move(join);
	// finally we remove the original equality expression from the filter
	return true;
}

bool ContainsCorrelatedExpressions(Expression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression&) expr;
		if (colref.depth > 0) {
			return true;
		}
	}
	bool contains_correlated_expressions = false;
	expr.EnumerateChildren([&](Expression *child) {
		if (ContainsCorrelatedExpressions(*child)) {
			contains_correlated_expressions = true;
			return;
		}
	});
	return contains_correlated_expressions;
}

bool ExtractCorrelatedExpressions(LogicalOperator *op, LogicalOperator *current_op, SubqueryExpression *subquery, size_t subquery_table_index,
                                  vector<JoinCondition> &join_conditions) {
	// we look through the subquery to find the matching correlation expression
	// using the Column Depth a correlating expression will have a column ref
	// with depth == 0 (belonging to the subquery) and a correlating expression
	// with depth == 1 (belonging to the main expression) we use the matcher to
	// find this comparison
	
	if (current_op->type != LogicalOperatorType::FILTER) {
		// not a filter, cannot extract from here
		// first check if there are any correlated expressions here
		// if there are we abandon the subquery rewriting
		for(auto &expr : current_op->expressions) {
			if (ContainsCorrelatedExpressions(*expr)) {
				return false;
			}
		}
		// if there are not 
		for(auto &child : current_op->children) {
			if (!ExtractCorrelatedExpressions(op, child.get(), subquery, subquery_table_index, join_conditions)) {
				return false;
			}
		}
		return true;
	}
	// filter, check for each expression if it is a comparison between a correlated and non-correlated expression
	for(size_t i = 0; i < current_op->expressions.size(); i++) {
		auto &expr = current_op->expressions[i];
		if (expr->GetExpressionClass() != ExpressionClass::COMPARISON) {
			// not a comparison, skip this entry
			continue;
		}
		auto sq_comp = (ComparisonExpression *)expr.get();
		if (sq_comp->left->type != ExpressionType::COLUMN_REF ||
			sq_comp->right->type != ExpressionType::COLUMN_REF) {
			// not a comparison between column refs, skip it
			continue;
		}
		auto sq_colref_left = (ColumnRefExpression *) sq_comp->left.get();
		auto sq_colref_right = (ColumnRefExpression *) sq_comp->right.get();
		if (sq_colref_left->depth == 0 && sq_colref_right->depth == 0) {
			// not a correlated comparison
			continue;
		}
		if (sq_colref_left->depth > 0 && sq_colref_right->depth > 0) {
			// comparison only between correlated columns
			continue;
		}
		bool is_inverted = false;
		ColumnRefExpression *sq_colref_inner, *sq_colref_outer;
		if (sq_colref_left->depth == 1) {
			sq_colref_inner = sq_colref_right;
			sq_colref_outer = sq_colref_left;
			is_inverted = true;
		} else if (sq_colref_right->depth == 1) {
			sq_colref_inner = sq_colref_left;
			sq_colref_outer = sq_colref_right;
		} else {
			// correlation goes up more than one level, abort
			continue;
		}


		auto comparison_type = sq_comp->type;

		auto comp_left = make_unique<ColumnRefExpression>(sq_colref_inner->return_type, sq_colref_inner->binding);
		auto comp_right = make_unique<ColumnRefExpression>(sq_colref_outer->return_type, sq_colref_outer->binding);

		// uncorrelated expression (depth = 0)
		auto uncorrelated_expression = !is_inverted ? move(sq_comp->left) : move(sq_comp->right);
		// correlated expression (depth = 1)
		auto correlated_expression = !is_inverted ? move(sq_comp->right) : move(sq_comp->left);

		if (op->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			auto aggr = (LogicalAggregate *)op;
			// now inside the aggregation, we use the uncorrelated column used
			// in the comparison as both projection and grouping col in subquery
			aggr->groups.push_back(move(uncorrelated_expression));
		} else {
			// push the expression into the select list
			assert(op->type == LogicalOperatorType::PROJECTION);
			op->expressions.push_back(move(uncorrelated_expression));
		}

		// remove the correlated expression from the filter in the subquery
		current_op->expressions.erase(current_op->expressions.begin() + i);

		// now we introduce a new join condition based on this correlated
		// expression
		JoinCondition condition;
		// on the left side is the original correlated expression
		// however, since there is no longer a subquery, its depth has changed
		// to 0
		((ColumnRefExpression *)correlated_expression.get())->depth = 0;
		condition.left = move(correlated_expression);
		// on the right side is the newly added group in the original subquery
		if (op->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			// in the case of the AGGREGATE_AND_GROUP_BY, we will add the right condition later on, as we need to
			// reference an expression in the PROJECTION above the AGGREGATE
			condition.right = nullptr;
		} else {
			condition.right = make_unique<ColumnRefExpression>(
			    op->expressions.back()->return_type, ColumnBinding(subquery_table_index, op->expressions.size() - 1));
			assert(condition.left->return_type == condition.right->return_type);
		}
		condition.comparison = comparison_type;
		if (!is_inverted) {
			// flip the comparison
			condition.comparison = ComparisonExpression::FlipComparisionExpression(condition.comparison);
		}

		// add the join condition
		join_conditions.push_back(move(condition));
		i--;
	}

	// check if there are any correlated column references left
	// if there are any we couldn't extract then the subquery cannot be rewritten
	for(auto &expr : current_op->expressions) {
		if (ContainsCorrelatedExpressions(*expr)) {
			return false;
		}
	}
	return true;
}
