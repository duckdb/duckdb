#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(SelectNode &statement) {
	unique_ptr<LogicalOperator> root;
	if (statement.from_table) {
		// SELECT with FROM
		root = CreatePlan(*statement.from_table);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.where_clause) {
		PlanSubqueries(&statement.where_clause, &root);
		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->AddChild(move(root));
		root = move(filter);
	}

	if (statement.HasAggregation()) {
		if (statement.HasGroup()) {
			// visit the groups
			for (size_t i = 0; i < statement.groups.size(); i++) {
				auto &group = statement.groups[i];
				PlanSubqueries(&group, &root);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : statement.binding.aggregates) {
			PlanSubqueries(&expr, &root);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate = make_unique<LogicalAggregate>(statement.binding.group_index, statement.binding.aggregate_index,
		                                               move(statement.binding.aggregates));
		aggregate->groups = move(statement.groups);

		aggregate->AddChild(move(root));
		root = move(aggregate);
	}

	if (statement.HasHaving()) {
		PlanSubqueries(&statement.having, &root);
		auto having = make_unique<LogicalFilter>(move(statement.having));

		having->AddChild(move(root));
		root = move(having);
	}

	if (statement.HasWindow()) {
		auto win = make_unique<LogicalWindow>(statement.binding.window_index);
		win->expressions = move(statement.binding.windows);
		// visit the window expressions
		for (auto &expr : win->expressions) {
			PlanSubqueries(&expr, &root);
		}
		assert(win->expressions.size() > 0);
		win->AddChild(move(root));
		root = move(win);
	}

	for (auto &expr : statement.select_list) {
		PlanSubqueries(&expr, &root);
	}

	// check if we need to prune extra columns that were introduced into the select list (by e.g. the ORDER BY or HAVING
	// clauses)
	bool prune_columns = statement.select_list.size() > statement.binding.column_count;

	// create the projection
	auto proj = make_unique<LogicalProjection>(statement.binding.projection_index, move(statement.select_list));
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	root = VisitQueryNode(statement, move(root));

	// add a prune node if necessary
	if (prune_columns) {
		assert(root);
		auto prune = make_unique<LogicalPruneColumns>(statement.binding.column_count);
		prune->AddChild(move(root));
		root = move(prune);
	}
	return move(root);
}
