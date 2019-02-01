#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

void Binder::Bind(SelectNode &statement) {
	if (statement.from_table) {
		AcceptChild(&statement.from_table);
	}

	// now we visit the rest of the statements
	// here we performing the binding of any mentioned column names
	// back to the tables/subqueries found in the FROM statement
	// (and throw an error if a mentioned column is not found)

	// first we visit the SELECT list
	// we generate a new list of expressions because a * statement expands to
	// multiple expressions
	// note that we also gather aliases from the SELECT list
	// because they might be used in the WHERE, GROUP BY or HAVING clauses
	vector<unique_ptr<Expression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context->GenerateAllColumnExpressions(new_select_list);
			continue;
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}

	statement.result_column_count = new_select_list.size();

	for (size_t i = 0; i < new_select_list.size(); i++) {
		auto &select_element = new_select_list[i];
		VisitExpression(&select_element);
		select_element->ResolveType();
		if (select_element->return_type == TypeId::INVALID) {
			throw BinderException("Could not resolve type of projection element!");
		}
	}

	// add alias references from SELECT list so they can be used in the ORDER BY clause
	for (size_t i = 0; i < new_select_list.size(); i++) {
		if (!new_select_list[i]->alias.empty()) {
			bind_context->AddExpression(new_select_list[i]->alias, new_select_list[i].get(), i);
		}
	}
	BindOrderBy(statement.orderby, new_select_list, statement.result_column_count);
	// for the ORDER BY statement, we have to project all the columns
	// in the projection phase as well
	// for each expression in the ORDER BY check if it is projected already
	// FIXME: should use hash map for equality comparisons
	for (size_t i = 0; i < statement.orderby.orders.size(); i++) {
		size_t j = 0;
		TypeId type = TypeId::INVALID;
		if (statement.orderby.orders[i].expression->type == ExpressionType::BOUND_REF) {
			// expression was already bound
			continue;
		}
		for (; j < new_select_list.size(); j++) {
			// check if the expression matches exactly
			if (statement.orderby.orders[i].expression->Equals(new_select_list[j].get())) {
				// in this case, we can just create a reference in the ORDER BY
				break;
			}
		}
		if (j == new_select_list.size()) {
			// if we didn't find a matching projection clause, we add it to the
			// projection list
			new_select_list.push_back(move(statement.orderby.orders[i].expression));
		}
		type = new_select_list[j]->return_type;
		if (type == TypeId::INVALID) {
			throw Exception("Could not deduce return type of ORDER BY expression");
		}
		statement.orderby.orders[i].expression = make_unique<BoundExpression>(type, j);
	}
	statement.select_list = move(new_select_list);

	if (statement.where_clause) {
		VisitExpression(&statement.where_clause);
		statement.where_clause->ResolveType();
	}

	if (statement.HasAggregation()) {
		if (statement.HasGroup()) {
			// bind group columns
			for (auto &group : statement.groupby.groups) {
				VisitExpression(&group);
				group->ResolveType();
			}

			// handle aliases in the GROUP BY columns
			for (size_t i = 0; i < statement.groupby.groups.size(); i++) {
				if (statement.groupby.groups[i]->type == ExpressionType::BOUND_REF) {
					// alias reference
					// move the computation here from the SELECT clause
					auto &bound_expr = (BoundExpression &)*statement.groupby.groups[i];
					auto select_index = bound_expr.index;
					auto group_ref = make_unique<BoundExpression>(statement.groupby.groups[i]->return_type, i);
					statement.groupby.groups[i] = move(statement.select_list[select_index]);
					group_ref->alias = statement.groupby.groups[i]->GetName();
					// and add a GROUP REF expression to the SELECT clause
					statement.select_list[select_index] = move(group_ref);
				}
			}
		}
	}

	if (statement.groupby.having) {
		VisitExpression(&statement.groupby.having);
		statement.groupby.having->ResolveType();
	}
}
