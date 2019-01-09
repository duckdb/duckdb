#include "planner/binder.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SQLStatement> Binder::Visit(SelectStatement &statement) {
	// first we visit the FROM statement
	// here we determine from where we can retrieve our columns (from which
	// tables/subqueries)

	// we also need to visit the CTEs because they generate table names
	for (auto &cte_it : statement.cte_map) {
		AddCTE(cte_it.first, cte_it.second.get());
	}
	// now visit the root node of the select statement
	statement.node->Accept(this);
	return nullptr;
}

static unique_ptr<Expression> replace_columns_with_group_refs(
    unique_ptr<Expression> expr,
    unordered_map<Expression *, size_t, ExpressionHashFunction, ExpressionEquality> &groups) {
	if (expr->GetExpressionClass() == ExpressionClass::AGGREGATE) {
		// already an aggregate, move it back
		return expr;
	}
	// check if the expression is a GroupBy expression
	auto entry = groups.find(expr.get());
	if (entry != groups.end()) {
		auto group = entry->first;
		// group reference! turn expression into a reference to the group
		auto group_ref = make_unique<ColumnRefExpression>(group->return_type, entry->second);
		group_ref->alias = expr->alias.empty() ? group->GetName() : expr->alias;
		return move(group_ref);
	}
	if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		// a column reference that is not contained inside an aggregate:
		auto select_column = (ColumnRefExpression *)expr.get();
		if (select_column->index != (size_t)-1) {
			// index already assigned, references a GROUP BY column
			return expr;
		}
		// check if the column references a group
		if (select_column->reference) {
			auto entry = groups.find(select_column->reference);
			if (entry != groups.end()) {
				// group reference!
				auto group_ref = make_unique<ColumnRefExpression>(entry->first->return_type, entry->second);
				group_ref->alias = string(select_column->column_name);
				return group_ref;
			}
		}
		// not a GROUP BY column and not part of an aggregate
		// create a FIRST aggregate around this aggregate
		string stmt_alias = expr->alias;
		auto first_aggregate = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(expr));
		first_aggregate->alias = stmt_alias;
		first_aggregate->ResolveType();
		return first_aggregate;
	}
	// not an aggregate and not a column reference
	// iterate over the children
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return replace_columns_with_group_refs(move(child), groups);
	});
	return expr;
}

void Binder::Visit(SelectNode &statement) {
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
		AcceptChild(&select_element);
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

	for (auto &order : statement.orderby.orders) {
		AcceptChild(&order.expression);
		if (order.expression->type == ExpressionType::COLUMN_REF) {
			auto selection_ref = reinterpret_cast<ColumnRefExpression *>(order.expression.get());
			if (selection_ref->column_name.empty()) {
				// this ORDER BY expression refers to a column in the select
				// clause by index e.g. ORDER BY 1 assign the type of the SELECT
				// clause
				if (selection_ref->index < 1 || selection_ref->index > statement.result_column_count) {
					throw BinderException("ORDER term out of range - should be between 1 and %d",
					                      (int)new_select_list.size());
				}
				selection_ref->return_type = new_select_list[selection_ref->index - 1]->return_type;
				selection_ref->reference = new_select_list[selection_ref->index - 1].get();
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
			if (statement.orderby.orders[i].expression->Equals(new_select_list[j].get())) {
				// in this case, we can just create a reference in the ORDER BY
				break;
			}
			// if the ORDER BY is an alias reference, check if the alias matches
			if (statement.orderby.orders[i].expression->type == ExpressionType::COLUMN_REF &&
			    reinterpret_cast<ColumnRefExpression *>(statement.orderby.orders[i].expression.get())->reference ==
			        new_select_list[j].get()) {
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
		statement.orderby.orders[i].expression = make_unique<ColumnRefExpression>(type, j);
	}
	statement.select_list = move(new_select_list);

	if (statement.where_clause) {
		AcceptChild(&statement.where_clause);
		statement.where_clause->ResolveType();
	}

	if (statement.HasGroup()) {
		// bind group columns
		for (auto &group : statement.groupby.groups) {
			AcceptChild(&group);
			group->ResolveType();
		}

		// handle aliases in the GROUP BY columns
		unordered_map<Expression *, size_t, ExpressionHashFunction, ExpressionEquality> groups;
		for (size_t i = 0; i < statement.groupby.groups.size(); i++) {
			if (statement.groupby.groups[i]->type == ExpressionType::COLUMN_REF) {
				auto group_column = reinterpret_cast<ColumnRefExpression *>(statement.groupby.groups[i].get());
				if (group_column->reference) {
					// alias reference
					// move the computation here from the SELECT clause
					size_t select_index = group_column->index;
					auto group_ref = make_unique<ColumnRefExpression>(statement.groupby.groups[i]->return_type, i);
					statement.groupby.groups[i] = move(statement.select_list[select_index]);
					group_ref->alias = statement.groupby.groups[i]->GetName();
					// and add a GROUP REF expression to the SELECT clause
					statement.select_list[select_index] = move(group_ref);
				}
			}
			groups[statement.groupby.groups[i].get()] = i;
		}

		// handle column references in the SELECT clause that are not part of aggregates
		// we either replace with GROUP REFERENCES, or add an implicit FIRST() aggregation over them
		for (size_t i = 0; i < statement.select_list.size(); i++) {
			statement.select_list[i] = replace_columns_with_group_refs(move(statement.select_list[i]), groups);
		}
	}

	if (statement.groupby.having) {
		AcceptChild(&statement.groupby.having);
		statement.groupby.having->ResolveType();
	}
}

void Binder::Visit(SetOperationNode &statement) {
	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	assert(statement.left);
	assert(statement.right);

	Binder binder_left(context, this);
	Binder binder_right(context, this);

	statement.left->Accept(&binder_left);
	statement.setop_left_binder = move(binder_left.bind_context);

	statement.right->Accept(&binder_right);
	statement.setop_right_binder = move(binder_right.bind_context);

	// now handle the ORDER BY
	// get the selection list from one of the children, since a SetOp does not have its own selection list
	auto &select_list = statement.GetSelectList();
	for (auto &order : statement.orderby.orders) {
		AcceptChild(&order.expression);
		if (order.expression->type == ExpressionType::COLUMN_REF) {
			auto selection_ref = (ColumnRefExpression *)order.expression.get();
			if (selection_ref->column_name.empty()) {
				// this ORDER BY expression refers to a column in the select
				// clause by index e.g. ORDER BY 1 assign the type of the SELECT
				// clause
				if (selection_ref->index < 1 || selection_ref->index > select_list.size()) {
					throw BinderException("ORDER term out of range - should be between 1 and %d",
					                      (int)select_list.size());
				}
				selection_ref->return_type = select_list[selection_ref->index - 1]->return_type;
				selection_ref->reference = select_list[selection_ref->index - 1].get();
				selection_ref->index = selection_ref->index - 1;
			}
		}
		order.expression->ResolveType();
	}
}

unique_ptr<SQLStatement> Binder::Visit(InsertStatement &statement) {
	if (statement.select_statement) {
		AcceptChild(&statement.select_statement);
	}
	// visit the expressions
	for (auto &expression_list : statement.values) {
		for (auto &expression : expression_list) {
			AcceptChild(&expression);
		}
	}
	return nullptr;
}

unique_ptr<SQLStatement> Binder::Visit(CopyStatement &stmt) {
	if (stmt.select_statement) {
		stmt.select_statement->Accept(this);
	}
	return nullptr;
}

unique_ptr<SQLStatement> Binder::Visit(CreateIndexStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// visit the expressions
	for (auto &expr : stmt.expressions) {
		AcceptChild(&expr);
		expr->ResolveType();
		if (expr->return_type == TypeId::INVALID) {
			throw BinderException("Could not resolve type of projection element!");
		}
		expr->ClearStatistics();
	}
	return nullptr;
}

unique_ptr<SQLStatement> Binder::Visit(DeleteStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// project any additional columns required for the condition
	if (stmt.condition) {
		AcceptChild(&stmt.condition);
	}
	return nullptr;
}

unique_ptr<SQLStatement> Binder::Visit(AlterTableStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	return nullptr;
}

unique_ptr<SQLStatement> Binder::Visit(UpdateStatement &stmt) {
	// visit the table reference
	AcceptChild(&stmt.table);
	// project any additional columns required for the condition/expressions
	if (stmt.condition) {
		AcceptChild(&stmt.condition);
	}
	for (auto &expression : stmt.expressions) {
		AcceptChild(&expression);
		if (expression->type == ExpressionType::VALUE_DEFAULT) {
			// we resolve the type of the DEFAULT expression in the
			// LogicalPlanGenerator because that is where we resolve the
			// to-be-updated column
			continue;
		}
		expression->ResolveType();
		if (expression->return_type == TypeId::INVALID) {
			throw BinderException("Could not resolve type of projection element!");
		}
	}
	return nullptr;
}

unique_ptr<SQLStatement> Binder::Visit(CreateTableStatement &stmt) {
	// bind any constraints
	// first create a fake table
	bind_context->AddDummyTable(stmt.info->table, stmt.info->columns);
	for (auto &it : stmt.info->constraints) {
		AcceptChild(&it);
	}
	return nullptr;
}

unique_ptr<Constraint> Binder::Visit(CheckConstraint &constraint) {
	SQLNodeVisitor::Visit(constraint);

	constraint.expression->ResolveType();
	if (constraint.expression->return_type == TypeId::INVALID) {
		throw BinderException("Could not resolve type of constraint!");
	}
	// the CHECK constraint should always return an INTEGER value
	if (constraint.expression->return_type != TypeId::INTEGER) {
		constraint.expression = make_unique<CastExpression>(TypeId::INTEGER, move(constraint.expression));
	}
	return nullptr;
}

unique_ptr<Expression> Binder::Visit(ColumnRefExpression &expr) {
	if (expr.column_name.empty()) {
		// column expression should have been bound already
		return nullptr;
	}
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (expr.table_name.empty()) {
		// no table name: find a binding that contains this
		expr.table_name = bind_context->GetMatchingBinding(expr.column_name);
	}
	bind_context->BindColumn(expr);
	return nullptr;
}

unique_ptr<Expression> Binder::Visit(FunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	expr.bound_function =
	    context.db.catalog.GetScalarFunction(context.ActiveTransaction(), expr.schema, expr.function_name);
	return nullptr;
}

unique_ptr<Expression> Binder::Visit(SubqueryExpression &expr) {
	assert(bind_context);

	Binder binder(context, this);
	binder.bind_context->parent = bind_context.get();
	// the subquery may refer to CTEs from the parent query
	binder.CTE_bindings = CTE_bindings;

	expr.subquery->Accept(&binder);
	auto &select_list = expr.subquery->GetSelectList();
	if (select_list.size() < 1) {
		throw BinderException("Subquery has no projections");
	}
	if (select_list[0]->return_type == TypeId::INVALID) {
		throw BinderException("Subquery has no type");
	}
	if (expr.subquery_type == SubqueryType::IN && select_list.size() != 1) {
		throw BinderException("Subquery returns %zu columns - expected 1", select_list.size());
	}

	expr.return_type = expr.subquery_type == SubqueryType::EXISTS ? TypeId::BOOLEAN : select_list[0]->return_type;
	expr.context = move(binder.bind_context);
	expr.is_correlated = expr.context->GetMaxDepth() > 0;
	return nullptr;
}

// CTEs are also referred to using BaseTableRefs, hence need to distinguish
unique_ptr<TableRef> Binder::Visit(BaseTableRef &expr) {
	auto cte = FindCTE(expr.table_name);
	if (cte) {
		auto subquery = make_unique<SubqueryRef>(move(cte));
		subquery->alias = expr.alias.empty() ? expr.table_name : expr.alias;
		AcceptChild(&subquery);
		return move(subquery);
	}

	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), expr.schema_name, expr.table_name);
	bind_context->AddBaseTable(expr.alias.empty() ? expr.table_name : expr.alias, table);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(CrossProductRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(JoinRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	AcceptChild(&expr.condition);
	expr.condition->ResolveType();
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(SubqueryRef &expr) {
	Binder binder(context, this);
	expr.subquery->Accept(&binder);
	expr.context = move(binder.bind_context);

	bind_context->AddSubquery(expr.alias, expr);
	return nullptr;
}

unique_ptr<TableRef> Binder::Visit(TableFunction &expr) {
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);
	bind_context->AddTableFunction(expr.alias.empty() ? function_definition->function_name : expr.alias, function);
	return nullptr;
}

void Binder::AddCTE(const string &name, QueryNode *cte) {
	assert(cte);
	assert(!name.empty());
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		throw BinderException("Duplicate CTE \"%s\" in query!", name.c_str());
	}
	CTE_bindings[name] = cte;
}

unique_ptr<QueryNode> Binder::FindCTE(const string &name) {
	auto entry = CTE_bindings.find(name);
	if (entry == CTE_bindings.end()) {
		if (parent) {
			return parent->FindCTE(name);
		}
		return nullptr;
	}
	return entry->second->Copy();
}
