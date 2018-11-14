
#include "optimizer/logical_rules/exists_rewrite.hpp"
#include "optimizer/logical_rules/subquery_rewrite.hpp"
#include "optimizer/rewriter.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

ExistsRewriteRule::ExistsRewriteRule() {
	auto subquery = make_unique_base<AbstractRuleNode, ExpressionNodeType>(
	    ExpressionType::SELECT_SUBQUERY);

	vector<ExpressionType> types = {ExpressionType::OPERATOR_EXISTS,
	                                ExpressionType::OPERATOR_NOT_EXISTS};
	auto exists = make_unique_base<AbstractRuleNode, ExpressionNodeSet>(types);

	exists->children.push_back(move(subquery));
	exists->child_policy = ChildPolicy::ORDERED;

	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(
	    LogicalOperatorType::FILTER);

	root->children.push_back(move(exists));
	root->child_policy = ChildPolicy::SOME;
}

unique_ptr<LogicalOperator>
ExistsRewriteRule::Apply(Rewriter &rewriter, LogicalOperator &op_root,
                         vector<AbstractOperator> &bindings,
                         bool &fixed_point) {
	auto *filter = (LogicalFilter *)bindings[0].value.op;
	auto *exists = (OperatorExpression *)bindings[1].value.expr;
	auto *subquery = (SubqueryExpression *)bindings[2].value.expr;

	// Only rewrite correlated exists
	// Non-correlated exists should be rewritten in a different way
	// Because either (1) the whole result is empty, or (2) it's a NOP operation
	if (!subquery->is_correlated) {
		return nullptr;
	}

	// find the projection
	auto node = subquery->op.get();
	LogicalOperator *parent = nullptr;
	while (node->children.size() == 1 && !IsProjection(node->type)) {
		parent = node;
		node = node->children[0].get();
	}

	// figure out the join type
	JoinType type;
	if (exists->type == ExpressionType::OPERATOR_EXISTS) {
		type = JoinType::SEMI;
	} else { /* OPERATOR_NOT_EXISTS */
		assert(exists->type == ExpressionType::OPERATOR_NOT_EXISTS);
		type = JoinType::ANTI;
	}

	if (node->type != LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
		// replace with AGGREGATE_AND_GROUP_BY node
		// the select list is irrelevant, as we only care about existance
		vector<unique_ptr<Expression>> empty_list;
		auto aggregate = make_unique<LogicalAggregate>(move(empty_list));
		aggregate->children = move(node->children);
		node = aggregate.get();
		if (parent) {
			parent->children[0] = move(aggregate);
		} else {
			subquery->op = move(aggregate);
		}
	}
	auto aggr = (LogicalAggregate *)node;

	// now we turn a subquery in the WHERE clause into a "proper" subquery
	// hence we need to get a new table index from the BindContext
	auto subquery_table_index = rewriter.context.GenerateTableIndex();

	// step 2: find correlations to add to the list of join conditions
	vector<JoinCondition> join_conditions;
	ExtractCorrelatedExpressions(aggr, subquery, subquery_table_index,
	                             join_conditions);

	// unlike equality comparison with subquery we only have the correlated
	// expressions as join condition
	assert(join_conditions.size() > 0);
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
	assert(filter->children.size() == 1);

	auto table_subquery = make_unique<LogicalSubquery>(
	    subquery_table_index, aggr->expressions.size());
	table_subquery->children.push_back(move(subquery->op));

	auto join = make_unique<LogicalJoin>(type);
	join->children.push_back(move(filter->children[0]));
	join->children.push_back(move(table_subquery));
	join->conditions = move(join_conditions);

	filter->children[0] = move(join);

	// finally we remove the original equality expression from the filter
	for (size_t i = 0; i < filter->expressions.size(); i++) {
		if (filter->expressions[i].get() == exists) {
			filter->expressions.erase(filter->expressions.begin() + i);
			break;
		}
	}
	fixed_point = false;
	return nullptr;
}