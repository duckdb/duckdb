#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

static LogicalType ResolveWindowExpressionType(ExpressionType window_type, LogicalType child_type) {
	switch (window_type) {
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
		return LogicalType(LogicalTypeId::DOUBLE);
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_NTILE:
		return LogicalType::BIGINT;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
		D_ASSERT(child_type.id() != LogicalTypeId::INVALID); // "Window function needs an expression"
		return child_type;
	case ExpressionType::WINDOW_LEAD:
	default:
		D_ASSERT(window_type == ExpressionType::WINDOW_LAG || window_type == ExpressionType::WINDOW_LEAD);
		D_ASSERT(child_type.id() != LogicalTypeId::INVALID); // "Window function needs an expression"
		return child_type;
	}
}

static unique_ptr<Expression> GetExpression(unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return nullptr;
	}
	D_ASSERT(expr.get());
	D_ASSERT(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
	return move(((BoundExpression &)*expr).expr);
}

static unique_ptr<Expression> CastWindowExpression(unique_ptr<ParsedExpression> &expr, const LogicalType &type) {
	if (!expr) {
		return nullptr;
	}
	D_ASSERT(expr.get());
	D_ASSERT(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);

	auto &bound = (BoundExpression &)*expr;
	bound.expr = BoundCastExpression::AddCastToType(move(bound.expr), type);

	return move(bound.expr);
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
	vector<LogicalType> types;
	vector<unique_ptr<Expression>> children;
	for (auto &child : window.children) {
		D_ASSERT(child.get());
		D_ASSERT(child->expression_class == ExpressionClass::BOUND_EXPRESSION);
		auto &bound = (BoundExpression &)*child;
		// Add casts for positional arguments
		const auto argno = children.size();
		switch (window.type) {
		case ExpressionType::WINDOW_NTILE:
			// ntile(bigint)
			if (argno == 0) {
				bound.expr = BoundCastExpression::AddCastToType(move(bound.expr), LogicalType::BIGINT);
			}
			break;
		default:
			break;
		}
		types.push_back(bound.expr->return_type);
		children.push_back(move(bound.expr));
	}
	//  Determine the function type.
	LogicalType sql_type;
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	if (window.type == ExpressionType::WINDOW_AGGREGATE) {
		//  Look up the aggregate function in the catalog
		auto func =
		    (AggregateFunctionCatalogEntry *)Catalog::GetCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
		        context, window.schema, window.function_name);
		if (func->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			throw BinderException("Unknown windowed aggregate");
		}
		// bind the aggregate
		string error;
		auto best_function = Function::BindFunction(func->name, func->functions, types, error);
		if (best_function == INVALID_INDEX) {
			throw BinderException(binder.FormatError(window, error));
		}
		// found a matching function! bind it as an aggregate
		auto &bound_function = func->functions[best_function];
		auto bound_aggregate = AggregateFunction::BindAggregateFunction(context, bound_function, move(children));
		// create the aggregate
		aggregate = make_unique<AggregateFunction>(bound_aggregate->function);
		bind_info = move(bound_aggregate->bind_info);
		children = move(bound_aggregate->children);
		sql_type = bound_aggregate->return_type;
	} else {
		// fetch the child of the non-aggregate window function (if any)
		sql_type = ResolveWindowExpressionType(window.type, types.empty() ? LogicalType() : types[0]);
	}
	auto result = make_unique<BoundWindowExpression>(window.type, sql_type, move(aggregate), move(bind_info));
	result->children = move(children);
	for (auto &child : window.partitions) {
		result->partitions.push_back(GetExpression(child));
	}
	auto &config = DBConfig::GetConfig(context);
	for (auto &order : window.orders) {
		auto type = order.type == OrderType::ORDER_DEFAULT ? config.default_order_type : order.type;
		auto null_order =
		    order.null_order == OrderByNullType::ORDER_DEFAULT ? config.default_null_order : order.null_order;
		auto expression = GetExpression(order.expression);
		result->orders.emplace_back(type, null_order, move(expression));
	}

	result->start_expr = CastWindowExpression(window.start_expr, LogicalType::BIGINT);
	result->end_expr = CastWindowExpression(window.end_expr, LogicalType::BIGINT);
	result->offset_expr = CastWindowExpression(window.offset_expr, LogicalType::BIGINT);
	result->default_expr = CastWindowExpression(window.default_expr, result->return_type);
	result->start = window.start;
	result->end = window.end;

	// create a BoundColumnRef that references this entry
	auto colref = make_unique<BoundColumnRefExpression>(window.GetName(), result->return_type,
	                                                    ColumnBinding(node.window_index, node.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.windows.push_back(move(result));
	return BindResult(move(colref));
}

} // namespace duckdb
