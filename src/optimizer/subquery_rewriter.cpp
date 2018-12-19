#include "optimizer/subquery_rewriter.hpp"

#include "optimizer/rewriter.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

void ExtractCorrelatedExpressions(LogicalOperator *op, SubqueryExpression *subquery, size_t subquery_table_index,
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
			case ExpressionType::COMPARE_NOT_IN:
				// (NOT) IN, check if we are dealing with a subquery
				if (expr.children.size() != 2) {
					// only constant lists have multiple elements
					break;
				}
				if (expr.children[1]->type == ExpressionType::SELECT_SUBQUERY) {
					// IN (SUBQUERY), rewrite
					if (RewriteInClause(filter, (OperatorExpression *)filter.expressions[i].get(),
					                    (SubqueryExpression *)expr.children[1].get())) {
						// successful rewrite, remove the expression from the set of filter expressions
						filter.expressions.erase(filter.expressions.begin() + i);
						i--;
					}
				}
				break;
			case ExpressionType::OPERATOR_EXISTS:
			case ExpressionType::OPERATOR_NOT_EXISTS:
				// NOT (EXISTS), check if we are dealing with a subquery
				assert(expr.children.size() == 1);
				if (expr.children[0]->type == ExpressionType::SELECT_SUBQUERY) {
					// EXISTS (SUBQUERY), rewrite
					if (RewriteExistsClause(filter, (OperatorExpression *)filter.expressions[i].get(),
					                        (SubqueryExpression *)expr.children[0].get())) {
						// successful rewrite, remove the expression from the set of filter expressions
						filter.expressions.erase(filter.expressions.begin() + i);
						i--;
					}
				}
				break;
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOTEQUAL:
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				// comparison, check if we are dealing with a subquery
				assert(expr.children.size() == 2);
				if (expr.children[0]->type == ExpressionType::SELECT_SUBQUERY ||
				    expr.children[1]->type == ExpressionType::SELECT_SUBQUERY) {
					int subquery_index = expr.children[0]->type == ExpressionType::SELECT_SUBQUERY ? 0 : 1;
					// Comparison with subquery, rewrite
					if (RewriteSubqueryComparison(filter, (ComparisonExpression *)filter.expressions[i].get(),
					                              (SubqueryExpression *)expr.children[subquery_index].get())) {
						// successful rewrite, remove the expression from the set of filter expressions
						filter.expressions.erase(filter.expressions.begin() + i);
						i--;
					}
				}
				break;
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
	ExtractCorrelatedExpressions(node, subquery, subquery_table_index, join_conditions);

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

bool SubqueryRewriter::RewriteSubqueryComparison(LogicalFilter &filter, ComparisonExpression *comparison,
                                                 SubqueryExpression *subquery) {
	// rewrite a comparison with a subquery (e.g. A == (SUBQUERY))
	// step 1: check that subquery is a projection
	auto node = GetProjection(subquery->op.get());
	if (node->type != LogicalOperatorType::PROJECTION) {
		return false;
	}
	auto proj = (LogicalProjection *)node;

	// now we turn a subquery in the WHERE clause into a "proper" subquery
	// hence we need to get a new table index from the BindContext
	auto subquery_table_index = context.GenerateTableIndex();

	// step 2: find correlations to add to the list of join conditions
	vector<JoinCondition> join_conditions;
	ExtractCorrelatedExpressions(proj, subquery, subquery_table_index, join_conditions);

	// create the join conditions
	// first is the original condition
	JoinCondition condition;
	condition.left =
	    subquery == comparison->children[0].get() ? move(comparison->children[1]) : move(comparison->children[0]);
	// the right condition is the first column of the subquery
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

void ExtractCorrelatedExpressions(LogicalOperator *op, SubqueryExpression *subquery, size_t subquery_table_index,
                                  vector<JoinCondition> &join_conditions) {
	// we look through the subquery to find the matching correlation expression
	// using the Column Depth a correlating expression will have a column ref
	// with depth == 0 (belonging to the subquery) and a correlating expression
	// with depth == 1 (belonging to the main expression) we use the matcher to
	// find this comparison
	auto equality_rule = make_unique_base<AbstractRuleNode, ComparisonNodeType>();
	equality_rule->children.push_back(make_unique<ColumnRefNodeDepth>(0));
	equality_rule->children.push_back(make_unique<ColumnRefNodeDepth>(1));
	equality_rule->child_policy = ChildPolicy::SOME;

	auto filter_rule = make_unique<LogicalNodeType>(LogicalOperatorType::FILTER);
	filter_rule->children.push_back(move(equality_rule));
	filter_rule->child_policy = ChildPolicy::SOME;

	while (true) {
		// search for a correlating comparison
		// this is a comparison like e.g. a.A = b.A
		// where "a" is from the subquery, and "b" from the outer layer
		vector<AbstractOperator> sq_bindings;
		auto subquery_op = AbstractOperator(op);
		for (auto it = subquery_op.begin(); it != subquery_op.end(); it++) {
			if (Rewriter::MatchOperands(filter_rule.get(), *it, sq_bindings)) {
				break;
			}
		}
		if (sq_bindings.size() == 0) {
			// correlated comparison operator not found inside subquery anymore
			break;
		}

		auto *sq_filter = (LogicalFilter *)sq_bindings[0].value.op;
		auto *sq_comp = (ComparisonExpression *)sq_bindings[1].value.expr;
		auto *sq_colref_inner = (ColumnRefExpression *)sq_bindings[2].value.expr;
		auto *sq_colref_outer = (ColumnRefExpression *)sq_bindings[3].value.expr;
		auto comparison_type = sq_comp->type;

		auto comp_left =
		    make_unique_base<Expression, ColumnRefExpression>(sq_colref_inner->return_type, sq_colref_inner->binding);
		auto comp_right =
		    make_unique_base<Expression, ColumnRefExpression>(sq_colref_outer->return_type, sq_colref_outer->binding);

		// index of uncorrelated expression (0 or 1)
		size_t uncorrelated_index = sq_comp->children[0].get() == sq_colref_inner ? 0 : 1;
		auto uncorrelated_expression = move(sq_comp->children[uncorrelated_index]);
		// correlated expression
		auto correlated_expression = move(sq_comp->children[1 - uncorrelated_index]);

		if (op->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			auto aggr = (LogicalAggregate *)op;
			// now inside the aggregation, we use the uncorrelated column used
			// in the comparison as both projection and grouping col in subquery
			aggr->expressions.push_back(make_unique_base<Expression, GroupRefExpression>(
			    uncorrelated_expression->return_type, aggr->groups.size()));
			aggr->groups.push_back(move(uncorrelated_expression));
		} else {
			// push the expression into the select list
			assert(op->type == LogicalOperatorType::PROJECTION);
			op->expressions.push_back(move(uncorrelated_expression));
		}

		// remove the correlated expression from the filter in the subquery
		for (size_t i = 0; i < sq_filter->expressions.size(); i++) {
			if (sq_filter->expressions[i].get() == sq_comp) {
				sq_filter->expressions.erase(sq_filter->expressions.begin() + i);
				break;
			}
		}

		// now we introduce a new join condition based on this correlated
		// expression
		JoinCondition condition;
		// on the left side is the original correlated expression
		// however, since there is no longer a subquery, its depth has changed
		// to 0
		((ColumnRefExpression *)correlated_expression.get())->depth = 0;
		condition.left = move(correlated_expression);
		// on the right side is the newly added aggregate in the original
		// subquery
		condition.right = make_unique<ColumnRefExpression>(
		    op->expressions.back()->return_type, ColumnBinding(subquery_table_index, op->expressions.size() - 1));
		condition.comparison = comparison_type;
		if (uncorrelated_index == 0) {
			// flip the comparison
			condition.comparison = ComparisonExpression::FlipComparisionExpression(condition.comparison);
		}

		// add the join condition
		join_conditions.push_back(move(condition));
	}
}
