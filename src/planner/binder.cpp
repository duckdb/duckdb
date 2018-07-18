
#include "planner/binder.hpp"

#include "parser/expression/basetableref_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/join_expression.hpp"
#include "parser/expression/subquery_expression.hpp"

#include "parser/statement/select_statement.hpp"

using namespace duckdb;
using namespace std;

void Binder::Visit(SelectStatement &statement) {
	context = make_shared<BindContext>();
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
	if (statement.where_clause) {
		statement.where_clause->Accept(this);
	}
	for(auto& group : statement.groupby.groups) {
		group->Accept(this);
	}
	if (statement.groupby.having) {
		statement.groupby.having->Accept(this);
	}
	for(auto& order : statement.orderby.orders) {
		order.expression->Accept(this);
	}
	// we generate a new list of expressions because a * statement expands to multiple expressions
	vector<unique_ptr<AbstractExpression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			context->GenerateAllColumnExpressions(new_select_list);
			continue;
		}
		// regular statement, visit it and add it to the list
		select_element->Accept(this);
		new_select_list.push_back(move(select_element));
	}
	statement.select_list = move(new_select_list);
}

void Binder::Visit(BaseTableRefExpression &expr) {
	auto table = catalog.GetTable(expr.schema_name, expr.table_name);
	context->AddBaseTable(expr.alias.empty() ? expr.table_name : expr.alias,
	                      table);
}

void Binder::Visit(ColumnRefExpression &expr) {
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (expr.table_name.empty()) {
		// no table name: find a table or subquery that contains this
		expr.table_name = context->GetMatchingTable(expr.column_name);
	}
	context->BindColumn(expr.table_name, expr.column_name);
}

void Binder::Visit(JoinExpression &expr) {
	expr.left->Accept(this);
	expr.right->Accept(this);
	expr.condition->Accept(this);
}

void Binder::Visit(SubqueryExpression &expr) {
	context->AddSubquery(expr.alias, expr.subquery.get());
	throw NotImplementedException("Binding subqueries not implemented yet!");
}
