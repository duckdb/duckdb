#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"

#include "planner/expression_binder/group_binder.hpp"
#include "planner/expression_binder/having_binder.hpp"
#include "planner/expression_binder/order_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"
#include "planner/expression_binder/where_binder.hpp"

#include <unordered_set>

using namespace duckdb;
using namespace std;


void Binder::Bind(SelectNode &statement) {
#ifdef DEBUG
	// a binder can only process a single SelectNode statement
	assert(!encountered_select_node);
	encountered_select_node = true;
#endif
	// first visit the FROM table statement
	if (statement.from_table) {
		AcceptChild(&statement.from_table);
	}

	if (statement.HasHaving() && !statement.HasGroup()) {
		throw ParserException("a GROUP BY clause is required before HAVING");
	}

	// visit the select list and expand any "*" statements
	vector<unique_ptr<Expression>> new_select_list;
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

	auto &binding = statement.binding;
	binding.column_count = statement.select_list.size();
	binding.projection_index = GenerateTableIndex();
	binding.group_index = GenerateTableIndex();
	binding.aggregate_index = GenerateTableIndex();
	binding.window_index = GenerateTableIndex();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		WhereBinder where_binder(*this, context);
		where_binder.BindAndResolveType(&statement.where_clause);
	}

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	unordered_map<string, uint32_t> alias_map;
	expression_map_t<uint32_t> projection_map;
	for(size_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
		}
		projection_map[expr.get()] = i;
	}

	// we bind the ORDER BY before we bind any aggregations or window functions
	for(size_t i = 0; i < statement.orderby.orders.size(); i++) {
		OrderBinder order_binder(*this, context, statement, alias_map, projection_map);
		auto result = order_binder.BindExpression(move(statement.orderby.orders[i].expression), 0);
		if (result.HasError()) {
			throw BinderException(result.error);
		}
		if (!result.expression) {
			// ORDER BY non-integer constant
			// remove the expression from the ORDER BY list
			statement.orderby.orders.erase(statement.orderby.orders.begin() + i);
			i--;
			continue;
		}
		assert(result.expression->type == ExpressionType::BOUND_COLUMN_REF);
		statement.orderby.orders[i].expression = move(result.expression);
	}
	
	vector<unique_ptr<Expression>> unbound_groups;
	expression_map_t<uint32_t> group_map;
	unordered_map<string, uint32_t> group_alias_map;
	if (statement.HasGroup()) {
		unordered_set<string> used_aliases;
		// the statement has a GROUP BY clause
		// columns in GROUP BY clauses:
		// FIRST refer to the original tables, and
		// THEN if no match is found refer to aliases in the SELECT list
		unbound_groups.resize(statement.groupby.groups.size());
		GroupBinder group_binder(*this, context, statement);
		for(size_t i = 0; i < statement.groupby.groups.size(); i++) {
			unbound_groups[i] = statement.groupby.groups[i]->Copy();

			// first try to bind using the GroupBinder
			auto result = group_binder.TryBindAndResolveType(move(statement.groupby.groups[i]));
			if (result.HasError()) {
				// failed to bind, check if it is an alias reference
				if (result.expression->type != ExpressionType::COLUMN_REF) {
					// not an alias reference
					throw BinderException(result.error);
				}
				auto &colref = (ColumnRefExpression&) *result.expression;
				if (!colref.table_name.empty()) {
					// explicit table name: not an alias reference
					throw BinderException(result.error);
				}
				auto entry = alias_map.find(colref.column_name);
				if (entry == alias_map.end()) {
					// no matching alias found
					throw BinderException(result.error);
				}
				// the group points towards an alias, check if the alias has been used already
				if (used_aliases.find(colref.column_name) != used_aliases.end()) {
					//  the alias has already been bound to before!
					// this only happens if we group on the same alias twice (e.g. GROUP BY k, k)
					// in this case, we can just remove the entry as grouping on the same entry twice has no additional effect
					statement.groupby.groups.erase(statement.groupby.groups.begin() + i);
					i--;
					continue;
				} else {
					unbound_groups[i] = statement.select_list[entry->second]->Copy();
					// in this case we have to move the computation of the GROUP BY column into this expression
					// move the expression into the group column and bind it
					statement.groupby.groups[i] = move(statement.select_list[entry->second]);
					group_binder.BindAndResolveType(&statement.groupby.groups[i]);
					// now replace the original expression in the select list with a reference to this group
					statement.select_list[entry->second] = make_unique<BoundColumnRefExpression>(*statement.groupby.groups[i], statement.groupby.groups[i]->return_type, ColumnBinding(binding.group_index, i), 0);
					// insert into the set of used aliases
					used_aliases.insert(colref.column_name);
					group_alias_map[colref.column_name] = i;
				}
			} else {
				statement.groupby.groups[i] = move(result.expression);
			}
			assert(statement.groupby.groups[i]->return_type != TypeId::INVALID);
			group_map[unbound_groups[i].get()] = i;
		}
	}

	// bind the HAVING clause, if any
	if (statement.HasHaving()) {
		HavingBinder having_binder(*this, context, statement, group_map, group_alias_map);
		having_binder.BindAndResolveType(&statement.groupby.having);
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, statement, group_map, group_alias_map);
	for(size_t i = 0; i < statement.select_list.size(); i++) {
		select_binder.BindAndResolveType(&statement.select_list[i]);
		statement.types.push_back(statement.select_list[i]->return_type);
	}
	// in the normal select binder, we bind columns as if there is no aggregation
	// i.e. in the query [SELECT i, SUM(i) FROM integers;] the "i" will be bound as a normal column
	// since we have an aggregation, we need to either (1) throw an error, or (2) wrap the column in a FIRST() aggregate
	// we choose the former one [CONTROVERSIAL: this is the PostgreSQL behavior]
	if (statement.HasAggregation()) {
		if (select_binder.bound_columns.size() > 0) {
			throw BinderException("column %s must appear in the GROUP BY clause or be used in an aggregate function", select_binder.bound_columns[0].c_str());
		}
	}

	// finally resolve the types of the ORDER BY clause
	for(size_t i = 0; i < statement.orderby.orders.size(); i++) {
		assert(statement.orderby.orders[i].expression->type == ExpressionType::BOUND_COLUMN_REF);
		auto &order = (BoundColumnRefExpression&) *statement.orderby.orders[i].expression;
		assert(order.binding.column_index < statement.select_list.size());
		order.return_type = statement.select_list[order.binding.column_index]->return_type;
		assert(order.return_type != TypeId::INVALID);
	}

}