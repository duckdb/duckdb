
#include "planner/binder.hpp"

#include "parser/expression/expression_list.hpp"
#include "parser/tableref/tableref_list.hpp"

#include "parser/statement/select_statement.hpp"

using namespace duckdb;
using namespace std;

void Binder::Visit(SelectStatement &statement) {
	// first we visit the FROM statement
	// here we determine from where we can retrieve our columns (from which
	// tables/subqueries)
	if (statement.from_table) {
		statement.from_table->Accept(this);
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
	vector<unique_ptr<AbstractExpression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			context->GenerateAllColumnExpressions(new_select_list);
			continue;
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}

	for (size_t i = 0; i < new_select_list.size(); i++) {
		auto &select_element = new_select_list[i];
		select_element->Accept(this);
		select_element->ResolveType();
		if (select_element->return_type == TypeId::INVALID) {
			throw BinderException(
			    "Could not resolve type of projection element!");
		}

		if (!select_element->alias.empty()) {
			context->AddExpression(select_element->alias, select_element.get(),
			                       i);
		}
	}
	for (auto &order : statement.orderby.orders) {
		order.expression->Accept(this);
		if (order.expression->type == ExpressionType::COLUMN_REF) {
			auto selection_ref =
			    reinterpret_cast<ColumnRefExpression *>(order.expression.get());
			if (selection_ref->column_name.empty()) {
				// this ORDER BY expression refers to a column in the select
				// clause by index e.g. ORDER BY 1 assign the type of the SELECT
				// clause
				if (selection_ref->index < 1 ||
				    selection_ref->index > new_select_list.size()) {
					throw BinderException(
					    "ORDER term out of range - should be between 1 and %d",
					    (int)new_select_list.size());
				}
				selection_ref->return_type =
				    new_select_list[selection_ref->index - 1]->return_type;
				selection_ref->reference =
				    new_select_list[selection_ref->index - 1].get();
			}
		}
		order.expression->ResolveType();
	}
	// for the ORDER BY statement, we have to project all the columns
	// in the projection phase as well
	// for each expression in the ORDER BY check if it is projected already
	for (size_t i = 0; i < statement.orderby.orders.size(); i++) {
		size_t j = 0;
		TypeId type = TypeId::INVALID;
		for (; j < new_select_list.size(); j++) {
			// check if the expression matches exactly
			if (statement.orderby.orders[i].expression->Equals(
			        new_select_list[j].get())) {
				// in this case, we can just create a reference in the ORDER BY
				break;
			}
			// if the ORDER BY is an alias reference, check if the alias matches
			if (statement.orderby.orders[i].expression->type ==
			        ExpressionType::COLUMN_REF &&
			    reinterpret_cast<ColumnRefExpression *>(
			        statement.orderby.orders[i].expression.get())
			            ->reference == new_select_list[j].get()) {
				break;
			}
		}
		if (j == new_select_list.size()) {
			// if we didn't find a matching projection clause, we add it to the
			// projection list
			new_select_list.push_back(
			    move(statement.orderby.orders[i].expression));
		}
		type = new_select_list[j]->return_type;
		if (type == TypeId::INVALID) {
			throw Exception(
			    "Could not deduce return type of ORDER BY expression");
		}
		statement.orderby.orders[i].expression =
		    make_unique<ColumnRefExpression>(type, j);
	}
	statement.select_list = move(new_select_list);

	if (statement.where_clause) {
		statement.where_clause->Accept(this);
	}

	if (statement.HasGroup()) {
		// bind group columns
		for (auto &group : statement.groupby.groups) {
			group->Accept(this);
		}

		// handle aliases in the GROUP BY columns
		for (size_t i = 0; i < statement.groupby.groups.size(); i++) {
			auto &group = statement.groupby.groups[i];
			if (group->type != ExpressionType::COLUMN_REF) {
				throw BinderException(
				    "GROUP BY clause needs to be a column or alias reference.");
			}
			auto group_column =
			    reinterpret_cast<ColumnRefExpression *>(group.get());
			if (group_column->reference) {
				// alias reference
				// move the computation here from the SELECT clause
				size_t select_index = group_column->index;
				statement.groupby.groups[i] =
				    move(statement.select_list[select_index]);
				// and add a GROUP REF expression to the SELECT clause
				statement.select_list[select_index] =
				    make_unique<GroupRefExpression>(
				        statement.groupby.groups[i]->return_type, i);
			}
		}

		// handle GROUP BY columns in the select clause
		for (size_t i = 0; i < statement.select_list.size(); i++) {
			auto &select = statement.select_list[i];
			if (select->type == ExpressionType::GROUP_REF)
				continue;
			if (select->IsAggregate())
				continue;

			// not an aggregate or existing GROUP REF, must point to a GROUP BY
			// column
			if (select->type != ExpressionType::COLUMN_REF) {
				// Every ColumnRef should point to a group by column OR alias
				throw Exception("SELECT with GROUP BY can only contain "
				                "aggregates or references to group columns!");
			}
			auto select_column =
			    reinterpret_cast<ColumnRefExpression *>(select.get());
			bool found_matching = false;
			for (size_t j = 0; j < statement.groupby.groups.size(); j++) {
				auto &group = statement.groupby.groups[j];
				if (select_column->reference) {
					if (select_column->reference == group.get()) {
						found_matching = true;
						statement.select_list[i] =
						    make_unique<GroupRefExpression>(
						        statement.select_list[i]->return_type, j);
						break;
					}
				} else {
					if (group->type == ExpressionType::COLUMN_REF) {
						auto group_column =
						    reinterpret_cast<ColumnRefExpression *>(
						        group.get());
						if (group_column->index == select_column->index) {
							statement.select_list[i] =
							    make_unique<GroupRefExpression>(
							        statement.select_list[i]->return_type, j);
							found_matching = true;
							break;
						}
					}
				}
			}
			if (!found_matching) {
				throw Exception("SELECT with GROUP BY can only contain "
				                "aggregates or references to group columns!");
			}
		}
	}

	if (statement.groupby.having) {
		statement.groupby.having->Accept(this);
	}
}

void Binder::Visit(ColumnRefExpression &expr) {
	if (expr.column_name.empty()) {
		// column expression should have been bound already
		return;
	}
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (expr.table_name.empty()) {
		// no table name: find a table or subquery that contains this
		expr.table_name = context->GetMatchingTable(expr.column_name);
	}
	auto column = context->BindColumn(expr);
}

void Binder::Visit(SubqueryExpression &expr) {
	assert(context);
	auto old_context = move(context);
	context = make_unique<BindContext>();
	context->parent = old_context.get();

	expr.subquery->Accept(this);
	if (expr.subquery->select_list.size() < 1) {
		throw BinderException("Subquery has no projections");
	}
	if (expr.subquery->select_list[0]->return_type == TypeId::INVALID) {
		throw BinderException("Subquery has no type");
	}
	expr.return_type = expr.exists ? TypeId::BOOLEAN
	                               : expr.subquery->select_list[0]->return_type;
	expr.context = move(context);
	context = move(old_context);
}

void Binder::Visit(BaseTableRef &expr) {
	auto table = catalog.GetTable(expr.schema_name, expr.table_name);
	context->AddBaseTable(expr.alias.empty() ? expr.table_name : expr.alias,
	                      table);
}

void Binder::Visit(CrossProductRef &expr) {
	expr.left->Accept(this);
	expr.right->Accept(this);
}

void Binder::Visit(JoinRef &expr) {
	expr.left->Accept(this);
	expr.right->Accept(this);
	expr.condition->Accept(this);
}

void Binder::Visit(SubqueryRef &expr) {
	context->AddSubquery(expr.alias, expr.subquery.get());
	throw NotImplementedException("Binding subqueries not implemented yet!");
}
