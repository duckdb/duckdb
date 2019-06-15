#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"
#include "planner/query_node/bound_select_node.hpp"
#include "planner/operator/logical_expression_get.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundSelectNode &statement) {
	unique_ptr<LogicalOperator> root;
	if (statement.from_table) {
		// SELECT with FROM
		root = CreatePlan(*statement.from_table);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.values.size() > 0) {
		// values list, first plan any subqueries in the list
		for (auto &expr_list : statement.values) {
			for (auto &expr : expr_list) {
				PlanSubqueries(&expr, &root);
			}
		}
		// now create a LogicalExpressionGet from the set of expressions
		// fetch the types
		vector<TypeId> types;
		for (auto &expr : statement.values[0]) {
			types.push_back(expr->return_type);
		}
		auto expr_get = make_unique<LogicalExpressionGet>(statement.projection_index, types, move(statement.values));
		expr_get->AddChild(move(root));
		return move(expr_get);
	}

	if (statement.where_clause) {
		PlanSubqueries(&statement.where_clause, &root);
		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->AddChild(move(root));
		root = move(filter);
	}

	if (statement.aggregates.size() > 0 || statement.groups.size() > 0) {
		if (statement.groups.size() > 0) {
			// visit the groups
			for (index_t i = 0; i < statement.groups.size(); i++) {
				auto &group = statement.groups[i];
				PlanSubqueries(&group, &root);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : statement.aggregates) {
			PlanSubqueries(&expr, &root);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate =
		    make_unique<LogicalAggregate>(statement.group_index, statement.aggregate_index, move(statement.aggregates));
		aggregate->groups = move(statement.groups);

		aggregate->AddChild(move(root));
		root = move(aggregate);
	}

	if (statement.having) {
		PlanSubqueries(&statement.having, &root);
		auto having = make_unique<LogicalFilter>(move(statement.having));

		having->AddChild(move(root));
		root = move(having);
	}

	if (statement.windows.size() > 0) {
		auto win = make_unique<LogicalWindow>(statement.window_index);
		win->expressions = move(statement.windows);
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
	bool prune_columns = statement.select_list.size() > statement.column_count;

	// create the projection
	auto proj = make_unique<LogicalProjection>(statement.projection_index, move(statement.select_list));
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	root = VisitQueryNode(statement, move(root));

	// add a prune node if necessary
	if (prune_columns) {
		assert(root);
		auto prune = make_unique<LogicalPruneColumns>(statement.column_count);
		prune->AddChild(move(root));
		root = move(prune);
	}
	return root;
}
