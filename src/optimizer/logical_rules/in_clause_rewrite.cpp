#include "optimizer/logical_rules/in_clause_rewrite.hpp"

#include "optimizer/rewriter.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

InClauseRewriteRule::InClauseRewriteRule() {
	auto subquery = make_unique_base<AbstractRuleNode, ExpressionNodeType>(ExpressionType::SELECT_SUBQUERY);

	vector<ExpressionType> types = {ExpressionType::COMPARE_IN, ExpressionType::COMPARE_NOT_IN};
	auto in_expression = make_unique_base<AbstractRuleNode, ExpressionNodeSet>(types);

	in_expression->children.push_back(make_unique<ExpressionNodeAny>());
	in_expression->children.push_back(move(subquery));
	in_expression->child_policy = ChildPolicy::ORDERED;

	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(LogicalOperatorType::FILTER);

	root->children.push_back(move(in_expression));
	root->child_policy = ChildPolicy::SOME;
}

unique_ptr<LogicalOperator> InClauseRewriteRule::Apply(Rewriter &rewriter, LogicalOperator &op_root,
                                                       vector<AbstractOperator> &bindings, bool &fixed_point) {
	auto *filter = (LogicalFilter *)bindings[0].value.op;
	auto *operator_expression = (OperatorExpression *)bindings[1].value.expr;
	auto *subquery = (SubqueryExpression *)bindings[3].value.expr;

	// we found a COMPARE_IN/COMPARE_NOT_IN with
	if (subquery->is_correlated) {
		// correlated subqueries not handled yet for in clause rewrite
		return nullptr;
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
	auto subquery_table_index = rewriter.context.GenerateTableIndex();

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
	join->children.push_back(move(filter->children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions.push_back(move(condition));

	// finally we remove the original in-expression from the filter
	for (size_t i = 0; i < filter->expressions.size(); i++) {
		if (filter->expressions[i].get() == operator_expression) {
			filter->expressions.erase(filter->expressions.begin() + i);
			break;
		}
	}
	filter->children[0] = move(join);
	fixed_point = false;
	return nullptr;
}
