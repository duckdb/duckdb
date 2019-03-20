#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/group_binder.hpp"
#include "planner/expression_binder/having_binder.hpp"
#include "planner/expression_binder/order_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"
#include "planner/expression_binder/where_binder.hpp"

#include "planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundQueryNode> Binder::Bind(SelectNode &statement) {
	auto result = make_unique<BoundSelectNode>();
	// first bind the FROM table statement
	if (statement.from_table) {
		result->from_table = Bind(*statement.from_table);
	}
	
	// visit the select list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context.GenerateAllColumnExpressions(new_select_list);
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}
	statement.select_list = move(new_select_list);

	result->column_count = statement.select_list.size();
	result->projection_index = GenerateTableIndex();
	result->group_index = GenerateTableIndex();
	result->aggregate_index = GenerateTableIndex();
	result->window_index = GenerateTableIndex();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		WhereBinder where_binder(*this, context);
		result->where_clause = where_binder.BindAndResolveType(statement.where_clause);
	}

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	unordered_map<string, uint32_t> alias_map;
	expression_map_t<uint32_t> projection_map;
	for (size_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
		}
		projection_map[expr.get()] = i;
	}

	// we bind the ORDER BY before we bind any aggregations or window functions
	for (size_t i = 0; i < statement.orders.size(); i++) {
		OrderBinder order_binder(*this, context, statement, alias_map, projection_map);
		auto result = order_binder.BindExpression(move(statement.orders[i].expression), 0);
		if (result.HasError()) {
			throw BinderException(result.error);
		}
		if (!result.expression) {
			// ORDER BY non-integer constant
			// remove the expression from the ORDER BY list
			statement.orders.erase(statement.orders.begin() + i);
			i--;
			continue;
		}
		assert(result.expression->type == ExpressionType::BOUND_COLUMN_REF);
		statement.orders[i].expression = move(result.expression);
	}

	vector<unique_ptr<Expression>> unbound_groups;
	expression_map_t<uint32_t> group_map;
	unordered_map<string, uint32_t> group_alias_map;
	if (statement.HasGroup()) {
		// the statement has a GROUP BY clause, bind it
		unbound_groups.resize(statement.groups.size());
		GroupBinder group_binder(*this, context, statement, alias_map, group_alias_map);
		for (size_t i = 0; i < statement.groups.size(); i++) {
			// we keep a copy of the unbound expression;
			// we keep the unbound copy around to check for group references in the SELECT and HAVING clause
			// the reason we want the unbound copy is because we want to figure out whether an expression
			// is a group reference BEFORE binding in the SELECT/HAVING binder
			group_binder.unbound_expression = statement.groups[i]->Copy();

			// bind the groups
			group_binder.bind_index = i;
			group_binder.BindAndResolveType(&statement.groups[i]);
			assert(statement.groups[i]->return_type != TypeId::INVALID);

			// in the unbound expression we DO bind the table names of any ColumnRefs
			// we do this to make sure that "table.a" and "a" are treated the same
			// if we wouldn't do this then (SELECT test.a FROM test GROUP BY a) would not work because "test.a" <> "a"
			// hence we convert "a" -> "test.a" in the unbound expression
			unbound_groups[i] = move(group_binder.unbound_expression);
			group_binder.BindTableNames(*unbound_groups[i]);
			group_map[unbound_groups[i].get()] = i;
		}
	}

	// bind the HAVING clause, if any
	if (statement.HasHaving()) {
		HavingBinder having_binder(*this, context, statement, group_map, group_alias_map);
		having_binder.BindTableNames(*statement.having);
		having_binder.BindAndResolveType(&statement.having);
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, statement, group_map, group_alias_map);
	for (size_t i = 0; i < statement.select_list.size(); i++) {
		select_binder.BindTableNames(*statement.select_list[i]);
		select_binder.BindAndResolveType(&statement.select_list[i]);
		if (i < binding.column_count) {
			statement.types.push_back(statement.select_list[i]->return_type);
		}
	}
	// in the normal select binder, we bind columns as if there is no aggregation
	// i.e. in the query [SELECT i, SUM(i) FROM integers;] the "i" will be bound as a normal column
	// since we have an aggregation, we need to either (1) throw an error, or (2) wrap the column in a FIRST() aggregate
	// we choose the former one [CONTROVERSIAL: this is the PostgreSQL behavior]
	if (statement.HasAggregation()) {
		if (select_binder.bound_columns.size() > 0) {
			throw BinderException("column %s must appear in the GROUP BY clause or be used in an aggregate function",
			                      select_binder.bound_columns[0].c_str());
		}
	}

	// finally resolve the types of the ORDER BY clause
	for (size_t i = 0; i < statement.orders.size(); i++) {
		assert(statement.orders[i].expression->type == ExpressionType::BOUND_COLUMN_REF);
		auto &order = (BoundColumnRefExpression &)*statement.orders[i].expression;
		assert(order.binding.column_index < statement.select_list.size());
		order.return_type = statement.select_list[order.binding.column_index]->return_type;
		assert(order.return_type != TypeId::INVALID);
	}
	return move(result);
}
