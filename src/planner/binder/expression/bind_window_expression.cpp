#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

static SQLType ResolveWindowExpressionType(ExpressionType window_type, SQLType child_type) {
	switch (window_type) {
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
		return SQLType(SQLTypeId::DECIMAL);
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_NTILE:
		return SQLType::BIGINT;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
		assert(child_type.id != SQLTypeId::INVALID); // "Window function needs an expression"
		return child_type;
	case ExpressionType::WINDOW_LEAD:
	default:
		assert(window_type == ExpressionType::WINDOW_LAG || window_type == ExpressionType::WINDOW_LEAD);
		assert(child_type.id != SQLTypeId::INVALID); // "Window function needs an expression"
		return child_type;
	}
}

static unique_ptr<Expression> GetExpression(unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return nullptr;
	}
	assert(expr.get());
	assert(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
	return move(((BoundExpression &)*expr).expr);
}

BindResult SelectBinder::BindWindow(WindowExpression &window, idx_t depth) {
	if (inside_window) {
		throw BinderException("window function calls cannot be nested");
	}
	if (depth > 0) {
		throw BinderException("correlated columns in window functions not supported");
	}
	// bind inside the children of the window function
	// we set the inside_window flag to true to prevent binding nested window functions
	this->inside_window = true;
	string error;
	for (auto &child : window.children) {
		BindChild(child, depth, error);
	}
	for (auto &child : window.partitions) {
		BindChild(child, depth, error);
	}
	for (auto &order : window.orders) {
		BindChild(order.expression, depth, error);
	}
	BindChild(window.start_expr, depth, error);
	BindChild(window.end_expr, depth, error);
	BindChild(window.offset_expr, depth, error);
	BindChild(window.default_expr, depth, error);
	this->inside_window = false;
	if (!error.empty()) {
		// failed to bind children of window function
		return BindResult(error);
	}
	// successfully bound all children: create bound window function
	vector<SQLType> types;
	vector<unique_ptr<Expression>> children;
	for (auto &child : window.children) {
		assert(child.get());
		assert(child->expression_class == ExpressionClass::BOUND_EXPRESSION);
		auto &bound = (BoundExpression &)*child;
		types.push_back(bound.sql_type);
		children.push_back(GetExpression(child));
	}
	//  Determine the function type.
	SQLType sql_type;
	unique_ptr<AggregateFunction> aggregate;
	if (window.type == ExpressionType::WINDOW_AGGREGATE) {
		//  Look up the aggregate function in the catalog
		auto func =
		    (AggregateFunctionCatalogEntry *)Catalog::GetCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
		        context, window.schema, window.function_name);
		if (func->type != CatalogType::AGGREGATE_FUNCTION) {
			throw BinderException("Unknown windowed aggregate");
		}
		// bind the aggregate
		auto best_function = Function::BindFunction(func->name, func->functions, types);
		// found a matching function!
		auto &bound_function = func->functions[best_function];
		// check if we need to add casts to the children
		bound_function.CastToFunctionArguments(children, types);
		// create the aggregate
		aggregate = make_unique<AggregateFunction>(func->functions[best_function]);
		sql_type = aggregate->return_type;
	} else {
		// fetch the child of the non-aggregate window function (if any)
		sql_type = ResolveWindowExpressionType(window.type, types.empty() ? SQLType() : types[0]);
	}
	auto result = make_unique<BoundWindowExpression>(window.type, GetInternalType(sql_type), move(aggregate));
	result->children = move(children);
	for (auto &child : window.partitions) {
		result->partitions.push_back(GetExpression(child));
	}
	for (auto &order : window.orders) {
		BoundOrderByNode bound_order;
		bound_order.expression = GetExpression(order.expression);
		bound_order.type = order.type;
		result->orders.push_back(move(bound_order));
	}
	result->start_expr = GetExpression(window.start_expr);
	result->end_expr = GetExpression(window.end_expr);
	result->offset_expr = GetExpression(window.offset_expr);
	result->default_expr = GetExpression(window.default_expr);
	result->start = window.start;
	result->end = window.end;

	// create a BoundColumnRef that references this entry
	auto colref = make_unique<BoundColumnRefExpression>(window.GetName(), result->return_type,
	                                                    ColumnBinding(node.window_index, node.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.windows.push_back(move(result));
	return BindResult(move(colref), sql_type);
}
