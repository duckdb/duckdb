#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

static unique_ptr<Expression> ExtractAggregates(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                                 size_t aggregate_index) {
	if (expr->GetExpressionClass() == ExpressionClass::AGGREGATE) {
		auto colref_expr = make_unique<BoundColumnRefExpression>(*expr, expr->return_type, ColumnBinding(aggregate_index, result.size()));
		result.push_back(move(expr));
		return colref_expr;
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return ExtractAggregates(move(expr), result, aggregate_index);
	});
	return expr;
}

static unique_ptr<Expression> ExtractWindows(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                              size_t ngroups) {
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto colref_expr = make_unique<BoundExpression>(expr->return_type, ngroups + result.size());
		result.push_back(move(expr));
		return colref_expr;
	}

	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return ExtractWindows(move(expr), result, ngroups);
	});
	return expr;
}

static unique_ptr<Expression> ExtractGroupReferences(size_t group_index,
    unique_ptr<Expression> expr, expression_map_t<size_t> &groups) {
	assert(expr->GetExpressionClass() != ExpressionClass::AGGREGATE);

	// check if the expression is a GroupBy expression
	auto entry = groups.find(expr.get());
	if (entry != groups.end()) {
		auto group = entry->first;
		// group reference! turn expression into a reference to the group
		return make_unique<BoundColumnRefExpression>(*expr, group->return_type, ColumnBinding(group_index, entry->second));
	}
	// not an aggregate and not a column reference
	// iterate over the children
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ExtractGroupReferences(group_index, move(child), groups);
	});
	return expr;
}

static unique_ptr<Expression> WrapInFirstAggregate(size_t group_index,
	size_t aggregate_index, unique_ptr<Expression> expr, vector<unique_ptr<Expression>>& aggregates) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression&) *expr;
		if (colref.binding.table_index != group_index && colref.binding.table_index != aggregate_index) {
			// a column reference that does not refer to a GROUP column or AGGREGATE
			// create a colref that refers to this aggregate
			auto colref_expr = make_unique<BoundColumnRefExpression>(*expr, expr->return_type, ColumnBinding(aggregate_index, aggregates.size()));
			// create a FIRST aggregate around this column reference
			string stmt_alias = expr->alias;
			auto first_aggregate = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(expr));
			first_aggregate->alias = stmt_alias;
			first_aggregate->ResolveType();
			// add the FIRST aggregate to the set of aggregates
			aggregates.push_back(move(first_aggregate));
			return move(colref_expr);
		}
	} else {
		expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
			return WrapInFirstAggregate(group_index, aggregate_index, move(child), aggregates);
		});
	}
	return expr;
}

void LogicalPlanGenerator::CreatePlan(SelectNode &statement) {
	if (statement.from_table) {
		// SELECT with FROM
		AcceptChild(&statement.from_table);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.where_clause) {
		if (statement.where_clause->IsAggregate()) {
			throw ParserException("WHERE clause cannot contain aggregates!");
		}
		VisitExpression(&statement.where_clause);
		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->AddChild(move(root));
		root = move(filter);
	}

	if (statement.HasAggregation()) {
		vector<unique_ptr<Expression>> aggregates;
		auto group_index = bind_context.GenerateTableIndex();
		auto aggregate_index = bind_context.GenerateTableIndex();
		for (size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			statement.select_list[expr_idx] =
			    ExtractAggregates(move(statement.select_list[expr_idx]), aggregates, aggregate_index);
		}
		expression_map_t<size_t> groups;
		if (statement.HasGroup()) {
			// visit the groups
			// create a mapping of group -> group index
			for(size_t i = 0; i < statement.groupby.groups.size(); i++) {
				auto &group = statement.groupby.groups[i];
				VisitExpression(&group);
				groups[group.get()] = i;
			}
			// now check the HAVING clause and SELECT list for any references to groups
			// and replace them with BoundColumnRefExpressions
			for(size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
				statement.select_list[expr_idx] = ExtractGroupReferences(group_index, move(statement.select_list[expr_idx]), groups);
			}
		}
		if (statement.HasHaving()) {
			// turn any aggregates in the HAVING clause into BoundColumnRefExpression and move them into the LogicalAggregate
			statement.groupby.having = ExtractAggregates(move(statement.groupby.having), aggregates, aggregate_index);
			// turn any group references in the HAVING clause into BoundColumnRefExpression
			statement.groupby.having = ExtractGroupReferences(group_index, move(statement.groupby.having), groups);
		}
		// wrap column references that refer to non-group or aggregate columns into a FIRST aggregate
		for(size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			statement.select_list[expr_idx] = WrapInFirstAggregate(group_index, aggregate_index, move(statement.select_list[expr_idx]), aggregates);
		}
		// now visit all aggregate expressions
		for (auto &expr : aggregates) {
			VisitExpression(&expr);
		}
		// finally create the groups
		auto aggregate = make_unique<LogicalAggregate>(group_index, aggregate_index, move(aggregates));
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
		auto win = make_unique<LogicalWindow>();
		for (size_t expr_idx = 0; expr_idx < statement.select_list.size(); expr_idx++) {
			// FIXME find a better way of getting colcount of logical ops
			root->ResolveOperatorTypes();
			statement.select_list[expr_idx] =
			    ExtractWindows(move(statement.select_list[expr_idx]), win->expressions, root->types.size());
		}
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
	auto proj_index = bind_context.GenerateTableIndex();
	auto proj = make_unique<LogicalProjection>(proj_index, move(statement.select_list));
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	VisitQueryNode(statement);
}
