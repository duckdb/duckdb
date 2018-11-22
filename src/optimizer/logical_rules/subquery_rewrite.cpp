
#include "optimizer/logical_rules/subquery_rewrite.hpp"
#include "optimizer/rewriter.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

SubqueryRewritingRule::SubqueryRewritingRule() {
	auto subquery = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	    ExpressionType::SELECT_SUBQUERY);

	auto comparison = make_unique_base<AbstractRuleNode, ComparisonNodeType>();

	comparison->children.push_back(move(subquery));
	comparison->child_policy = ChildPolicy::SOME;

	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::FILTER);

	root->children.push_back(move(comparison));
	root->child_policy = ChildPolicy::SOME;
}

unique_ptr<LogicalOperator>
SubqueryRewritingRule::Apply(Rewriter &rewriter, LogicalOperator &op_root,
                             vector<AbstractOperator> &bindings,
                             bool &fixed_point) {
	auto *filter = (LogicalFilter *)bindings[0].value.op;
	auto *comparison = (ComparisonExpression *)bindings[1].value.expr;
	auto *subquery = (SubqueryExpression *)bindings[2].value.expr;

	// step 1: check that subquery is an aggregation
	auto node = GetProjection(subquery->op.get());
	if (node->type != LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
		return nullptr;
	}
	auto aggr = (LogicalAggregate *)node;

	// now we turn a subquery in the WHERE clause into a "proper" subquery
	// hence we need to get a new table index from the BindContext
	auto subquery_table_index = rewriter.context.GenerateTableIndex();

	// step 2: find correlations to add to the list of join conditions
	vector<JoinCondition> join_conditions;
	ExtractCorrelatedExpressions(aggr, subquery, subquery_table_index,
	                             join_conditions);

	// create the join conditions
	// first is the original condition
	JoinCondition condition;
	condition.left = subquery == comparison->children[0].get()
	                     ? move(comparison->children[1])
	                     : move(comparison->children[0]);
	// the right condition is the first column of the subquery
	auto &first_column = aggr->expressions[0];
	condition.right = make_unique<ColumnRefExpression>(
	    first_column->return_type, ColumnBinding(subquery_table_index, 0));
	condition.comparison = comparison->type;

	// now we add join between the filter and the subquery
	assert(filter->children.size() == 1);

	auto table_subquery = make_unique<LogicalSubquery>(
	    subquery_table_index, aggr->expressions.size());
	table_subquery->children.push_back(move(subquery->op));

	auto join = make_unique<LogicalJoin>(JoinType::INNER);
	join->children.push_back(move(filter->children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions.push_back(move(condition));
	for (auto &condition : join_conditions) {
		join->conditions.push_back(move(condition));
	}

	filter->children[0] = move(join);

	// finally we remove the original equality expression from the filter
	for (size_t i = 0; i < filter->expressions.size(); i++) {
		if (filter->expressions[i].get() == comparison) {
			filter->expressions.erase(filter->expressions.begin() + i);
			break;
		}
	}
	fixed_point = false;
	return nullptr;
}

namespace duckdb {

void ExtractCorrelatedExpressions(LogicalOperator *op,
                                  SubqueryExpression *subquery,
                                  size_t subquery_table_index,
                                  vector<JoinCondition> &join_conditions) {
	// we look through the subquery to find the matching correlation expression
	// using the Column Depth a correlating expression will have a column ref
	// with depth == 0 (belonging to the subquery) and a correlating expression
	// with depth == 1 (belonging to the main expression) we use the matcher to
	// find this comparison
	auto equality_rule =
	    make_unique_base<AbstractRuleNode, ComparisonNodeType>();
	equality_rule->children.push_back(make_unique<ColumnRefNodeDepth>(0));
	equality_rule->children.push_back(make_unique<ColumnRefNodeDepth>(1));
	equality_rule->child_policy = ChildPolicy::SOME;

	auto filter_rule =
	    make_unique<LogicalNodeType>(LogicalOperatorType::FILTER);
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
		auto *sq_colref_inner =
		    (ColumnRefExpression *)sq_bindings[2].value.expr;
		auto *sq_colref_outer =
		    (ColumnRefExpression *)sq_bindings[3].value.expr;
		auto comparison_type = sq_comp->type;

		auto comp_left = make_unique_base<Expression, ColumnRefExpression>(
		    sq_colref_inner->return_type, sq_colref_inner->binding);
		auto comp_right = make_unique_base<Expression, ColumnRefExpression>(
		    sq_colref_outer->return_type, sq_colref_outer->binding);

		// index of uncorrelated expression (0 or 1)
		size_t uncorrelated_index =
		    sq_comp->children[0].get() == sq_colref_inner ? 0 : 1;
		auto uncorrelated_expression =
		    move(sq_comp->children[uncorrelated_index]);
		// correlated expression
		auto correlated_expression =
		    move(sq_comp->children[1 - uncorrelated_index]);

		if (op->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			auto aggr = (LogicalAggregate *)op;
			// now inside the aggregation, we use the uncorrelated column used
			// in the comparison as both projection and grouping col in subquery
			aggr->expressions.push_back(
			    make_unique_base<Expression, GroupRefExpression>(
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
				sq_filter->expressions.erase(sq_filter->expressions.begin() +
				                             i);
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
		    op->expressions.back()->return_type,
		    ColumnBinding(subquery_table_index, op->expressions.size() - 1));
		condition.comparison = comparison_type;
		if (uncorrelated_index == 0) {
			// flip the comparison
			condition.comparison =
			    LogicalJoin::FlipComparisionExpression(condition.comparison);
		}

		// add the join condition
		join_conditions.push_back(move(condition));
	}
}

} // namespace duckdb
