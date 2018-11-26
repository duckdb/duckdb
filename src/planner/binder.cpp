
#include "planner/binder.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"

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
		select_element->Accept(this);
		select_element->ResolveType();
		if (select_element->return_type == TypeId::INVALID) {
			throw BinderException(
			    "Could not resolve type of projection element!");
		}

		if (!select_element->alias.empty()) {
			bind_context->AddExpression(select_element->alias,
			                            select_element.get(), i);
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
		statement.where_clause->ResolveType();
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
				auto group_ref = make_unique<GroupRefExpression>(
				    statement.groupby.groups[i]->return_type, i);
				group_ref->alias = string(group_column->column_name);
				statement.select_list[select_index] = move(group_ref);
			}
		}

		// handle GROUP BY columns in the select clause
		for (size_t i = 0; i < statement.select_list.size(); i++) {
			auto &select = statement.select_list[i];
			if (select->type == ExpressionType::GROUP_REF)
				continue;
			if (select->IsAggregate())
				continue;

			// not an aggregate or existing GROUP REF
			if (select->type == ExpressionType::COLUMN_REF) {
				// column reference: check if it points to a GROUP BY column
				auto select_column =
				    reinterpret_cast<ColumnRefExpression *>(select.get());
				bool found_matching = false;
				for (size_t j = 0; j < statement.groupby.groups.size(); j++) {
					auto &group = statement.groupby.groups[j];
					if (select_column->reference) {
						if (select_column->reference == group.get()) {
							// group reference!
							auto group_ref = make_unique<GroupRefExpression>(
							    statement.select_list[i]->return_type, j);
							group_ref->alias =
							    string(select_column->column_name);
							statement.select_list[i] = move(group_ref);
							found_matching = true;
							break;
						}
					} else {
						if (group->type == ExpressionType::COLUMN_REF) {
							auto group_column =
							    reinterpret_cast<ColumnRefExpression *>(
							        group.get());
							if (group_column->binding ==
							    select_column->binding) {
								auto group_ref =
								    make_unique<GroupRefExpression>(
								        statement.select_list[i]->return_type,
								        j);
								group_ref->alias =
								    string(select_column->column_name);
								statement.select_list[i] = move(group_ref);
								found_matching = true;
								break;
							}
						}
					}
				}
				if (found_matching) {
					// the column reference was turned into a GROUP BY reference
					// move to the next column
					continue;
				}
			}
			// not a group by column or aggregate
			// create a FIRST aggregate around this aggregate
			statement.select_list[i] = make_unique<AggregateExpression>(
			    ExpressionType::AGGREGATE_FIRST, false,
			    move(statement.select_list[i]));
			statement.select_list[i]->ResolveType();
			// throw Exception("SELECT with GROUP BY can only contain "
			//                 "aggregates or references to group columns!");
		}
	}

	if (statement.groupby.having) {
		statement.groupby.having->Accept(this);
		statement.groupby.having->ResolveType();
	}
	// the union has a completely independent binder
	if (statement.union_select) {
		Binder binder(context);
		binder.bind_context = make_unique<BindContext>();
		statement.union_select->Accept(&binder);
	}
}

void Binder::Visit(InsertStatement &statement) {
	if (statement.select_statement) {
		statement.select_statement->Accept(this);
	}
	// visit the expressions
	for (auto &expression_list : statement.values) {
		for (auto &expression : expression_list) {
			expression->Accept(this);
		}
	}
}

void Binder::Visit(CopyStatement &stmt) {
	if (stmt.select_statement) {
		stmt.select_statement->Accept(this);
	}
}

void Binder::Visit(DeleteStatement &stmt) {
	// visit the table reference
	stmt.table->Accept(this);
	// project any additional columns required for the condition
	stmt.condition->Accept(this);
}

void Binder::Visit(AlterTableStatement &stmt) {
	// visit the table reference
	stmt.table->Accept(this);
}

void Binder::Visit(UpdateStatement &stmt) {
	// visit the table reference
	stmt.table->Accept(this);
	// project any additional columns required for the condition/expressions
	if (stmt.condition) {
		stmt.condition->Accept(this);
	}
	for (auto &expression : stmt.expressions) {
		expression->Accept(this);
		if (expression->type == ExpressionType::VALUE_DEFAULT) {
			// we resolve the type of the DEFAULT expression in the
			// LogicalPlanGenerator because that is where we resolve the
			// to-be-updated column
			continue;
		}
		expression->ResolveType();
		if (expression->return_type == TypeId::INVALID) {
			throw BinderException(
			    "Could not resolve type of projection element!");
		}
	}
}

void Binder::Visit(CreateTableStatement &stmt) {
	// bind any constraints
	// first create a fake table
	bind_context->AddDummyTable(stmt.info->table, stmt.info->columns);
	for (auto &it : stmt.info->constraints) {
		it->Accept(this);
	}
}

void Binder::Visit(CheckConstraint &constraint) {
	SQLNodeVisitor::Visit(constraint);

	constraint.expression->ResolveType();
	if (constraint.expression->return_type == TypeId::INVALID) {
		throw BinderException("Could not resolve type of constraint!");
	}
	// the CHECK constraint should always return an INTEGER value
	if (constraint.expression->return_type != TypeId::INTEGER) {
		constraint.expression = make_unique<CastExpression>(
		    TypeId::INTEGER, move(constraint.expression));
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
		// no table name: find a binding that contains this
		expr.table_name = bind_context->GetMatchingBinding(expr.column_name);
	}
	bind_context->BindColumn(expr);
}

void Binder::Visit(FunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	expr.bound_function = context.db.catalog.GetScalarFunction(
	    context.ActiveTransaction(), expr.schema, expr.function_name);
}

void Binder::Visit(SubqueryExpression &expr) {
	assert(bind_context);

	Binder binder(context);
	binder.bind_context->parent = bind_context.get();

	expr.subquery->Accept(&binder);
	if (expr.subquery->select_list.size() < 1) {
		throw BinderException("Subquery has no projections");
	}
	if (expr.subquery->select_list[0]->return_type == TypeId::INVALID) {
		throw BinderException("Subquery has no type");
	}
	if (expr.subquery_type == SubqueryType::IN &&
	    expr.subquery->select_list.size() != 1) {
		throw BinderException("Subquery returns %zu columns - expected 1",
		                      expr.subquery->select_list.size());
	}

	expr.return_type = expr.subquery_type == SubqueryType::EXISTS
	                       ? TypeId::BOOLEAN
	                       : expr.subquery->select_list[0]->return_type;
	expr.context = move(binder.bind_context);
	expr.is_correlated = expr.context->GetMaxDepth() > 0;
}

void Binder::Visit(BaseTableRef &expr) {
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(),
	                                         expr.schema_name, expr.table_name);
	bind_context->AddBaseTable(
	    expr.alias.empty() ? expr.table_name : expr.alias, table);
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
	Binder binder(context);
	expr.subquery->Accept(&binder);
	expr.context = move(binder.bind_context);

	bind_context->AddSubquery(expr.alias, expr);
}

void Binder::Visit(TableFunction &expr) {
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(
	    context.ActiveTransaction(), function_definition);
	bind_context->AddTableFunction(
	    expr.alias.empty() ? function_definition->function_name : expr.alias,
	    function);
}
