#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::VisitQueryNode(QueryNode &statement) {
	assert(root);
	if (statement.select_distinct) {
		auto node = GetProjection(root.get());
		if (!IsProjection(node->type)) {
			throw Exception("DISTINCT can only apply to projection, union or group");
		}

		vector<unique_ptr<Expression>> expressions;
		vector<unique_ptr<Expression>> projections;
		vector<unique_ptr<Expression>> groups;

		for (size_t i = 0; i < node->expressions.size(); i++) {
			Expression *proj_ele = node->expressions[i].get();

			groups.push_back(make_unique_base<Expression, ColumnRefExpression>(proj_ele->return_type, i));
			auto colref = make_unique_base<Expression, ColumnRefExpression>(proj_ele->return_type, i);
			colref->alias = proj_ele->alias;
			projections.push_back(move(colref));
		}
		// this aggregate is superflous if all grouping columns are in aggr
		// below
		auto aggregate = make_unique<LogicalAggregate>(move(expressions));
		aggregate->groups = move(groups);
		aggregate->AddChild(move(root));
		root = move(aggregate);

		auto proj = make_unique<LogicalProjection>(move(projections));
		proj->AddChild(move(root));
		root = move(proj);
	}

	if (statement.HasOrder()) {
		auto order = make_unique<LogicalOrder>(move(statement.orderby));
		order->AddChild(move(root));
		root = move(order);
	}
	if (statement.HasLimit()) {
		auto limit = make_unique<LogicalLimit>(statement.limit.limit, statement.limit.offset);
		limit->AddChild(move(root));
		root = move(limit);
	}
}
