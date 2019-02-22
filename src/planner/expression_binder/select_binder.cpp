#include "planner/expression_binder/select_binder.hpp"

#include "main/client_context.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"

using namespace duckdb;
using namespace std;

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, SelectNode &node,
                           expression_map_t<uint32_t> &group_map, unordered_map<string, uint32_t> &group_alias_map)
    : SelectNodeBinder(binder, context, node), inside_window(false), group_map(group_map),
      group_alias_map(group_alias_map) {
}

BindResult SelectBinder::BindWindow(unique_ptr<Expression> expr, uint32_t depth) {
	assert(expr && expr->GetExpressionClass() == ExpressionClass::WINDOW);
	if (inside_window) {
		return BindResult(move(expr), "window function calls cannot be nested");
	}
	if (depth > 0) {
		return BindResult(move(expr), "correlated columns in window functions not supported");
	}
	// bind inside the children of the window function
	// we set the inside_window flag to true to prevent nested window functions
	this->inside_window = true;
	auto bind_result = BindChildren(move(expr), depth);
	this->inside_window = false;
	if (bind_result.HasError()) {
		// failed to bind children of window function
		return bind_result;
	}
	bind_result.expression->ResolveType();
	// create a BoundColumnRef that references this entry
	auto colref = make_unique<BoundColumnRefExpression>(
	    *bind_result.expression, bind_result.expression->return_type,
	    ColumnBinding(node.binding.window_index, node.binding.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.binding.windows.push_back(move(bind_result.expression));
	return BindResult(move(colref));
}

BindResult SelectBinder::BindColumnRef(unique_ptr<Expression> expr, uint32_t depth) {
	assert(expr && expr->GetExpressionClass() == ExpressionClass::COLUMN_REF);
	// bind the column using the normal binder
	auto colname = expr->GetName();
	auto result = BindColumnRefExpression(move(expr), depth);
	if (!result.HasError()) {
		// successfully bound the column to the base tables
		// add to the list of bound columns
		assert(result.expression->type == ExpressionType::BOUND_COLUMN_REF);
		bound_columns.push_back(colname);
	}
	return result;
}

BindResult SelectBinder::BindAggregate(unique_ptr<Expression> expr, uint32_t depth) {
	assert(expr && expr->GetExpressionClass() == ExpressionClass::AGGREGATE);
	// bind the children of the aggregation
	AggregateBinder aggregate_binder(binder, context, node);
	auto bind_result = aggregate_binder.BindChildren(move(expr), aggregate_binder.BoundColumns() ? depth : 0);
	if (aggregate_binder.BoundColumns() || !bind_result.HasError()) {
		// columns were bound or binding was successful!
		// that means this aggregation belongs to this node
		// check if we have to resolve any errors by binding with parent binders
		bind_result = BindCorrelatedColumns(move(bind_result), true);
		// if there is still an error after this, we could not successfully bind the aggregate
		if (bind_result.HasError()) {
			throw BinderException(bind_result.error);
		}
		bind_result.expression->ResolveType();
		ExtractCorrelatedExpressions(binder, *bind_result.expression);
		// successfully bound: extract the aggregation and place it inside the set of aggregates for this node
		auto colref = make_unique<BoundColumnRefExpression>(
		    *bind_result.expression, bind_result.expression->return_type,
		    ColumnBinding(node.binding.aggregate_index, node.binding.aggregates.size()), depth);
		// move the aggregate expression into the set of bound aggregates
		node.binding.aggregates.push_back(move(bind_result.expression));
		return BindResult(move(colref));
	}
	return bind_result;
}

unique_ptr<Expression> SelectBinder::TryBindGroup(Expression *expr, uint32_t depth) {
	uint32_t group_entry = (uint32_t)-1;
	bool found_group = false;
	// first check the group alias map, if expr is a ColumnRefExpression
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)*expr;
		if (colref.table_name.empty()) {
			auto alias_entry = group_alias_map.find(colref.column_name);
			if (alias_entry != group_alias_map.end()) {
				// found entry!
				group_entry = alias_entry->second;
				found_group = true;
			}
		}
	}
	// now check the list of group columns for a match
	if (!found_group) {
		auto entry = group_map.find(expr);
		if (entry != group_map.end()) {
			group_entry = entry->second;
			found_group = true;
		}
	}
	if (!found_group) {
		return nullptr;
	}
	return make_unique<BoundColumnRefExpression>(*expr, node.groupby.groups[group_entry]->return_type,
	                                             ColumnBinding(node.binding.group_index, group_entry), depth);
}

BindResult SelectBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	// check if the expression binds to one of the groups
	auto group_binding = TryBindGroup(expr.get(), depth);
	if (group_binding) {
		return BindResult(move(group_binding));
	}
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		return BindSubqueryExpression(move(expr), depth);
	case ExpressionClass::AGGREGATE:
		return BindAggregate(move(expr), depth);
	case ExpressionClass::WINDOW:
		return BindWindow(move(expr), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(move(expr), depth);
	case ExpressionClass::FUNCTION:
		return BindFunctionExpression(move(expr), depth);
	default:
		return BindChildren(move(expr), depth);
	}
}
