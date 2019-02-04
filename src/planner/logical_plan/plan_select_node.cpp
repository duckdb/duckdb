#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(SelectNode &statement) {
	if (statement.from_table) {
		// SELECT with FROM
		AcceptChild(&statement.from_table);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.where_clause) {
		VisitExpression(&statement.where_clause);
		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->AddChild(move(root));
		root = move(filter);
	}

	if (statement.HasAggregation()) {
		if (statement.HasGroup()) {
			// visit the groups
			for(size_t i = 0; i < statement.groupby.groups.size(); i++) {
				auto &group = statement.groupby.groups[i];
				VisitExpression(&group);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : statement.binding.aggregates) {
			VisitExpression(&expr);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate = make_unique<LogicalAggregate>(statement.binding.group_index, statement.binding.aggregate_index, move(statement.binding.aggregates));
		aggregate->groups = move(statement.groupby.groups);

		aggregate->AddChild(move(root));
		root = move(aggregate);
	}

	if (statement.HasHaving()) {
		VisitExpression(&statement.groupby.having);
		auto having = make_unique<LogicalFilter>(move(statement.groupby.having));

		having->AddChild(move(root));
		root = move(having);
	}

	if (statement.HasWindow()) {
		auto win = make_unique<LogicalWindow>(statement.binding.window_index);
		win->expressions = move(statement.binding.windows);
		// visit the window expressions
		for (auto &expr : win->expressions) {
			VisitExpression(&expr);
		}
		assert(win->expressions.size() > 0);
		win->AddChild(move(root));
		root = move(win);
	}

	for (auto &expr : statement.select_list) {
		VisitExpression(&expr);
	}
	auto proj = make_unique<LogicalProjection>(statement.binding.projection_index, move(statement.select_list));
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	VisitQueryNode(statement);
}
