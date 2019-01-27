#include "parser/expression/bound_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

static unique_ptr<Expression> extract_aggregates(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                                 size_t ngroups) {
	if (expr->GetExpressionClass() == ExpressionClass::AGGREGATE) {
		auto colref_expr = make_unique<BoundExpression>(expr->return_type, ngroups + result.size());
		result.push_back(move(expr));
		return move(colref_expr);
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return extract_aggregates(move(expr), result, ngroups);
	});
	return expr;
}

static unique_ptr<Expression> extract_windows(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                              size_t ngroups) {
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto colref_expr = make_unique<BoundExpression>(expr->return_type, ngroups + result.size());
		result.push_back(move(expr));
		return move(colref_expr);
	}

	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return extract_windows(move(expr), result, ngroups);
	});
	return expr;
}

void LogicalPlanGenerator::CreatePlan(SelectNode &statement) {
	for (auto &expr : statement.select_list) {
		VisitExpression(&expr);
	}

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
		vector<unique_ptr<Expression>> aggregates;

		// TODO: what about the aggregates in window partition/order/boundaries? use a visitor here?
		for (size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			statement.select_list[expr_idx] =
			    extract_aggregates(move(statement.select_list[expr_idx]), aggregates, statement.groupby.groups.size());
		}

		if (statement.HasHaving()) {
			VisitExpression(&statement.groupby.having);
			// the HAVING child cannot contain aggregates itself
			// turn them into Column References
			statement.groupby.having =
			    extract_aggregates(move(statement.groupby.having), aggregates, statement.groupby.groups.size());
		}

		auto aggregate = make_unique<LogicalAggregate>(move(aggregates));
		if (statement.HasGroup()) {
			// have to add group by columns
			aggregate->groups = move(statement.groupby.groups);
		}

		aggregate->AddChild(move(root));
		root = move(aggregate);

		if (statement.HasHaving()) {
			auto having = make_unique<LogicalFilter>(move(statement.groupby.having));

			having->AddChild(move(root));
			root = move(having);
		}
	}

	if (statement.HasWindow()) {
		auto win = make_unique<LogicalWindow>();
		for (size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			// FIXME find a better way of getting colcount of logical ops
			root->ResolveOperatorTypes();
			statement.select_list[expr_idx] =
			    extract_windows(move(statement.select_list[expr_idx]), win->expressions, root->types.size());
		}
		assert(win->expressions.size() > 0);
		win->AddChild(move(root));
		root = move(win);
	}

	auto proj = make_unique<LogicalProjection>(move(statement.select_list));
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	VisitQueryNode(statement);
}
