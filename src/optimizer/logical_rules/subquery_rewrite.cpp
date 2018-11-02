
#include "optimizer/logical_rules/subquery_rewrite.hpp"
#include "optimizer/rewriter.hpp"

using namespace duckdb;
using namespace std;

SubqueryRewritingRule::SubqueryRewritingRule() {
	auto subquery = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	    ExpressionType::SELECT_SUBQUERY);

	auto equal = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	    ExpressionType::COMPARE_EQUAL);

	equal->children.push_back(move(subquery));
	equal->child_policy = ChildPolicy::SOME;

	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::FILTER);

	root->children.push_back(move(equal));
	root->child_policy = ChildPolicy::SOME;
}

std::unique_ptr<LogicalOperator>
SubqueryRewritingRule::Apply(Rewriter &rewriter, LogicalOperator &op_root,
                             std::vector<AbstractOperator> &bindings) {
	auto *filter = (LogicalFilter *)bindings[0].value.op;
	auto *equal = (ComparisonExpression *)bindings[1].value.expr;
	auto *subquery = (SubqueryExpression *)bindings[2].value.expr;

	// step 1: check if there is a correlation in the subquery
	if (!subquery->is_correlated) {
		return nullptr;
	}

	// step 2: check that subquery is an aggregation
	if (subquery->op->type != LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
		return nullptr;
	}

	// step 3: find correlation
	// we look through the subquery to find the matching correlation expression
	// using the Column Depth a correlating expression will have a column ref
	// with depth == 0 (belonging to the subquery) and a correlating expression
	// with depth == 1 (belonging to the main expression) we use the matcher to
	// find this comparison
	auto sq_eq = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	    ExpressionType::COMPARE_EQUAL);

	sq_eq->children.push_back(make_unique<ColumnRefNodeDepth>(0));
	sq_eq->children.push_back(make_unique<ColumnRefNodeDepth>(1));
	sq_eq->child_policy = ChildPolicy::SOME;

	auto sq_root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::FILTER);

	sq_root->children.push_back(move(sq_eq));
	sq_root->child_policy = ChildPolicy::SOME;

	std::vector<AbstractOperator> sq_bindings;

	// FIXME: what if there are multiple correlations?
	auto aop = AbstractOperator(subquery->op.get());
	for (auto it = aop.begin(); it != aop.end(); it++) {
		if (Rewriter::MatchOperands(sq_root.get(), *it, sq_bindings)) {
			break;
		}
	}
	if (sq_bindings.size() == 0) {
		throw Exception("Could not find equality correlation comparision");
	}

	auto *sq_filter = (LogicalFilter *)sq_bindings[0].value.op;
	auto *sq_comp = (ComparisonExpression *)sq_bindings[1].value.expr;
	auto *sq_colref_inner = (ColumnRefExpression *)sq_bindings[2].value.expr;
	auto *sq_colref_outer = (ColumnRefExpression *)sq_bindings[3].value.expr;

	auto comp_left = make_unique_base<Expression, ColumnRefExpression>(
	    sq_colref_inner->return_type, sq_colref_inner->binding);
	auto comp_right = make_unique_base<Expression, ColumnRefExpression>(
	    sq_colref_outer->return_type, sq_colref_outer->binding);

	auto aggr = (LogicalAggregate *)subquery->op.get();

	// index of uncorrelated expression (0 or 1)
	size_t uncorrelated_index =
	    sq_comp->children[0].get() == sq_colref_inner ? 0 : 1;
	auto uncorrelated_expression = move(sq_comp->children[uncorrelated_index]);
	// correlated expression
	auto correlated_expression =
	    move(sq_comp->children[1 - uncorrelated_index]);

	// now inside the aggregation, we use the uncorrelated column used in the
	// comparison as both projection and grouping col in subquery
	aggr->expressions.push_back(
	    make_unique_base<Expression, GroupRefExpression>(
	        uncorrelated_expression->return_type, aggr->groups.size()));
	aggr->groups.push_back(move(uncorrelated_expression));

	// remove the correlated expression from the filter in the subquery
	for (size_t i = 0; i < sq_filter->expressions.size(); i++) {
		if (sq_filter->expressions[i].get() == sq_comp) {
			sq_filter->expressions.erase(sq_filter->expressions.begin() + i);
			break;
		}
	}

	// now we turn a subquery in the WHERE clause into a "proper" subquery
	// hence we need to get a new table index from the BindContext
	auto subquery_table_index = rewriter.context.GenerateTableIndex();

	// create the join conditions
	// first is the original condition
	JoinCondition original_condition;
	original_condition.left = subquery == equal->children[0].get()
	                              ? move(equal->children[1])
	                              : move(equal->children[0]);
	// the right condition is the first column of the subquery
	auto &first_column = subquery->op->expressions[0];
	original_condition.right = make_unique<ColumnRefExpression>(
	    first_column->return_type, ColumnBinding(subquery_table_index, 0));
	original_condition.comparison = ExpressionType::COMPARE_EQUAL;

	// now we introduce the new join condition
	JoinCondition condition;
	// on the left side is the original correlated expression
	// however, since there is no longer a subquery, its depth has changed to 0
	((ColumnRefExpression *)correlated_expression.get())->depth = 0;
	condition.left = move(correlated_expression);
	// on the right side is the newly added aggregate in the original subquery
	condition.right = make_unique<ColumnRefExpression>(
	    aggr->expressions.back()->return_type,
	    ColumnBinding(subquery_table_index, aggr->expressions.size() - 1));
	condition.comparison = ExpressionType::COMPARE_EQUAL;

	// now we add join between the filter and the subquery
	assert(filter->children.size() == 1);

	auto table_subquery = make_unique<LogicalSubquery>(
	    subquery_table_index, aggr->expressions.size());
	table_subquery->children.push_back(move(subquery->op));

	auto join = make_unique<LogicalJoin>(JoinType::INNER);
	join->children.push_back(move(filter->children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions.push_back(move(original_condition));
	join->conditions.push_back(move(condition));

	filter->children[0] = move(join);

	// finally we remove the original equality expression from the filter
	for (size_t i = 0; i < filter->expressions.size(); i++) {
		if (filter->expressions[i].get() == equal) {
			filter->expressions.erase(filter->expressions.begin() + i);
			break;
		}
	}

	return nullptr;
}