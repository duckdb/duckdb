#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/group_binder.hpp"
#include "duckdb/planner/expression_binder/having_binder.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/table_star_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundQueryNode> Binder::Bind(SelectNode &statement) {
	auto result = make_unique<BoundSelectNode>();
	result->projection_index = GenerateTableIndex();
	result->group_index = GenerateTableIndex();
	result->aggregate_index = GenerateTableIndex();
	result->window_index = GenerateTableIndex();

	// first bind the FROM table statement
	result->from_table = Bind(*statement.from_table);

	// visit the select list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context.GenerateAllColumnExpressions(new_select_list);
		} else if (select_element->GetExpressionType() == ExpressionType::TABLE_STAR) {
			auto table_star =
			    (TableStarExpression *)select_element.get(); // TODO this cast to explicit class is a bit dirty?
			bind_context.GenerateAllColumnExpressions(new_select_list, table_star->relation_name);
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}
	statement.select_list = move(new_select_list);

	for (auto &entry : statement.select_list) {
		result->names.push_back(entry->GetName());
	}
	result->column_count = statement.select_list.size();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		WhereBinder where_binder(*this, context);
		result->where_clause = where_binder.Bind(statement.where_clause);
	}

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	unordered_map<string, idx_t> alias_map;
	expression_map_t<idx_t> projection_map;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
		}
		projection_map[expr.get()] = i;
	}

	// we bind the DISTINCT ON before we bind any order, aggregations or window functions
	vector<unique_ptr<ParsedExpression>> extra_select_list;
	OrderBinder order_binder(result->projection_index, statement, alias_map, projection_map, extra_select_list);
	for (idx_t i = 0; i < statement.distinct_on_targets.size(); i++) {
		// we treat the Distinct list as a order by
		auto bound_expr = order_binder.Bind(move(statement.distinct_on_targets[i]));
		if (!bound_expr) {
			// DISTINCT ON non-integer constant
			// remove the expression from the  DISTINCT ON list
			continue;
		}
		assert(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
		result->target_distincts.push_back(move(bound_expr));
	}

	// we bind the ORDER BY before we bind any aggregations or window functions
	for (idx_t i = 0; i < statement.orders.size(); i++) {
		auto bound_expr = order_binder.Bind(move(statement.orders[i].expression));
		if (!bound_expr) {
			// ORDER BY non-integer constant
			// remove the expression from the ORDER BY list
			continue;
		}
		assert(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
		BoundOrderByNode order_node;
		order_node.expression = move(bound_expr);
		order_node.type = statement.orders[i].type;
		result->orders.push_back(move(order_node));
	}

	vector<unique_ptr<ParsedExpression>> unbound_groups;
	BoundGroupInformation info;
	if (statement.groups.size() > 0) {
		// the statement has a GROUP BY clause, bind it
		unbound_groups.resize(statement.groups.size());
		GroupBinder group_binder(*this, context, statement, result->group_index, alias_map, info.alias_map);
		for (idx_t i = 0; i < statement.groups.size(); i++) {

			// we keep a copy of the unbound expression;
			// we keep the unbound copy around to check for group references in the SELECT and HAVING clause
			// the reason we want the unbound copy is because we want to figure out whether an expression
			// is a group reference BEFORE binding in the SELECT/HAVING binder
			group_binder.unbound_expression = statement.groups[i]->Copy();
			group_binder.bind_index = i;

			// bind the groups
			SQLType group_type;
			auto bound_expr = group_binder.Bind(statement.groups[i], &group_type);
			assert(bound_expr->return_type != TypeId::INVALID);
			info.group_types.push_back(group_type);
			result->groups.push_back(move(bound_expr));

			// in the unbound expression we DO bind the table names of any ColumnRefs
			// we do this to make sure that "table.a" and "a" are treated the same
			// if we wouldn't do this then (SELECT test.a FROM test GROUP BY a) would not work because "test.a" <> "a"
			// hence we convert "a" -> "test.a" in the unbound expression
			unbound_groups[i] = move(group_binder.unbound_expression);
			group_binder.BindTableNames(*unbound_groups[i]);
			info.map[unbound_groups[i].get()] = i;
		}
	}

	// bind the HAVING clause, if any
	if (statement.having) {
		HavingBinder having_binder(*this, context, *result, info);
		having_binder.BindTableNames(*statement.having);
		result->having = having_binder.Bind(statement.having);
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, *result, info);
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		select_binder.BindTableNames(*statement.select_list[i]);
	}
	// including the extra SELECT list from DISTINCT ON & ORDER BY
	unordered_map<idx_t, idx_t> projection_remap;
	for (idx_t i = 0; i < extra_select_list.size(); i++) {
		auto &expr = extra_select_list[i];
		select_binder.BindTableNames(*expr);
		// See if it matches an entry already in the SELECT list
		auto post_bind_index = projection_map.find(expr.get());
		idx_t newindex;
		if (post_bind_index != projection_map.end()) {
			newindex = post_bind_index->second;
		} else {
			newindex = statement.select_list.size();
			statement.select_list.push_back(move(expr));
		}
		projection_remap[result->column_count + i] = newindex;
	}
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		SQLType result_type;
		auto expr = select_binder.Bind(statement.select_list[i], &result_type);
		result->select_list.push_back(move(expr));
		if (i < result->column_count) {
			result->types.push_back(result_type);
		}
	}
	// in the normal select binder, we bind columns as if there is no aggregation
	// i.e. in the query [SELECT i, SUM(i) FROM integers;] the "i" will be bound as a normal column
	// since we have an aggregation, we need to either (1) throw an error, or (2) wrap the column in a FIRST() aggregate
	// we choose the former one [CONTROVERSIAL: this is the PostgreSQL behavior]
	if (result->groups.size() > 0 || result->aggregates.size() > 0 || statement.having) {
		if (select_binder.BoundColumns()) {
			throw BinderException("column must appear in the GROUP BY clause or be used in an aggregate function");
		}
	}

	// resolve the types of the ORDER BY clause
	for (idx_t i = 0; i < result->orders.size(); i++) {
		assert(result->orders[i].expression->type == ExpressionType::BOUND_COLUMN_REF);
		auto &order = (BoundColumnRefExpression &)*result->orders[i].expression;
		// See if the column index has been remapped in the projection
		auto entry = projection_remap.find(order.binding.column_index);
		if (entry != projection_remap.end())
			order_binder.RemapIndex(order, entry->second);
		assert(order.binding.column_index < statement.select_list.size());
		order.return_type = result->select_list[order.binding.column_index]->return_type;
		assert(order.return_type != TypeId::INVALID);
	}

	// resolve the types of the DISTINCT ON clause
	for (idx_t i = 0; i < result->target_distincts.size(); i++) {
		assert(result->target_distincts[i]->type == ExpressionType::BOUND_COLUMN_REF);
		auto &distinct = (BoundColumnRefExpression &)*result->target_distincts[i];
		// See if the column index has been remapped in the projection
		auto entry = projection_remap.find(distinct.binding.column_index);
		if (entry != projection_remap.end())
			order_binder.RemapIndex(distinct, entry->second);
		assert(distinct.binding.column_index < statement.select_list.size());
		distinct.return_type = result->select_list[distinct.binding.column_index]->return_type;
		assert(distinct.return_type != TypeId::INVALID);
	}

	return move(result);
}
