#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_binder.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

static LogicalType ResolveWindowExpressionType(ExpressionType window_type, const vector<LogicalType> &child_types) {

	idx_t param_count;
	switch (window_type) {
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
		param_count = 0;
		break;
	case ExpressionType::WINDOW_NTILE:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		param_count = 1;
		break;
	case ExpressionType::WINDOW_NTH_VALUE:
		param_count = 2;
		break;
	default:
		throw InternalException("Unrecognized window expression type " + ExpressionTypeToString(window_type));
	}
	if (child_types.size() != param_count) {
		throw BinderException("%s needs %d parameter%s, got %d", ExpressionTypeToString(window_type), param_count,
		                      param_count == 1 ? "" : "s", child_types.size());
	}
	switch (window_type) {
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
		return LogicalType(LogicalTypeId::DOUBLE);
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_NTILE:
		return LogicalType::BIGINT;
	case ExpressionType::WINDOW_NTH_VALUE:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		return child_types[0];
	default:
		throw InternalException("Unrecognized window expression type " + ExpressionTypeToString(window_type));
	}
}

static inline OrderType ResolveOrderType(const DBConfig &config, OrderType type) {
	return (type == OrderType::ORDER_DEFAULT) ? config.options.default_order_type : type;
}

static inline OrderByNullType ResolveNullOrder(const DBConfig &config, OrderByNullType null_order) {
	return (null_order == OrderByNullType::ORDER_DEFAULT) ? config.options.default_null_order : null_order;
}

static unique_ptr<Expression> GetExpression(unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return nullptr;
	}
	D_ASSERT(expr.get());
	D_ASSERT(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
	return std::move(((BoundExpression &)*expr).expr);
}

static unique_ptr<Expression> CastWindowExpression(unique_ptr<ParsedExpression> &expr, const LogicalType &type) {
	if (!expr) {
		return nullptr;
	}
	D_ASSERT(expr.get());
	D_ASSERT(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);

	auto &bound = (BoundExpression &)*expr;
	bound.expr = BoundCastExpression::AddDefaultCastToType(std::move(bound.expr), type);

	return std::move(bound.expr);
}

static LogicalType BindRangeExpression(ClientContext &context, const string &name, unique_ptr<ParsedExpression> &expr,
                                       unique_ptr<ParsedExpression> &order_expr) {

	vector<unique_ptr<Expression>> children;

	D_ASSERT(order_expr.get());
	D_ASSERT(order_expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
	auto &bound_order = (BoundExpression &)*order_expr;
	children.emplace_back(bound_order.expr->Copy());

	D_ASSERT(expr.get());
	D_ASSERT(expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
	auto &bound = (BoundExpression &)*expr;
	children.emplace_back(std::move(bound.expr));

	string error;
	FunctionBinder function_binder(context);
	auto function = function_binder.BindScalarFunction(DEFAULT_SCHEMA, name, std::move(children), error, true);
	if (!function) {
		throw BinderException(error);
	}
	bound.expr = std::move(function);
	return bound.expr->return_type;
}

BindResult SelectBinder::BindWindow(WindowExpression &window, idx_t depth) {
	auto name = window.GetName();

	QueryErrorContext error_context(binder.root_statement, window.query_location);
	if (inside_window) {
		throw BinderException(error_context.FormatError("window function calls cannot be nested"));
	}
	if (depth > 0) {
		throw BinderException(error_context.FormatError("correlated columns in window functions not supported"));
	}
	// If we have range expressions, then only one order by clause is allowed.
	if ((window.start == WindowBoundary::EXPR_PRECEDING_RANGE || window.start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	     window.end == WindowBoundary::EXPR_PRECEDING_RANGE || window.end == WindowBoundary::EXPR_FOLLOWING_RANGE) &&
	    window.orders.size() != 1) {
		throw BinderException(error_context.FormatError("RANGE frames must have only one ORDER BY expression"));
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
	BindChild(window.filter_expr, depth, error);
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
				bound.expr = BoundCastExpression::AddCastToType(context, std::move(bound.expr), LogicalType::BIGINT);
			}
			break;
		case ExpressionType::WINDOW_NTH_VALUE:
			// nth_value(<expr>, index)
			if (argno == 1) {
				bound.expr = BoundCastExpression::AddCastToType(context, std::move(bound.expr), LogicalType::BIGINT);
			}
		default:
			break;
		}
		types.push_back(bound.expr->return_type);
		children.push_back(std::move(bound.expr));
	}
	//  Determine the function type.
	LogicalType sql_type;
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	if (window.type == ExpressionType::WINDOW_AGGREGATE) {
		//  Look up the aggregate function in the catalog
		auto func = Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, window.catalog, window.schema,
		                                                             window.function_name, false, error_context);
		D_ASSERT(func->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

		// bind the aggregate
		string error;
		FunctionBinder function_binder(context);
		auto best_function = function_binder.BindFunction(func->name, func->functions, types, error);
		if (best_function == DConstants::INVALID_INDEX) {
			throw BinderException(binder.FormatError(window, error));
		}
		// found a matching function! bind it as an aggregate
		auto bound_function = func->functions.GetFunctionByOffset(best_function);
		auto bound_aggregate = function_binder.BindAggregateFunction(bound_function, std::move(children));
		// create the aggregate
		aggregate = make_unique<AggregateFunction>(bound_aggregate->function);
		bind_info = std::move(bound_aggregate->bind_info);
		children = std::move(bound_aggregate->children);
		sql_type = bound_aggregate->return_type;
	} else {
		// fetch the child of the non-aggregate window function (if any)
		sql_type = ResolveWindowExpressionType(window.type, types);
	}
	auto result = make_unique<BoundWindowExpression>(window.type, sql_type, std::move(aggregate), std::move(bind_info));
	result->children = std::move(children);
	for (auto &child : window.partitions) {
		result->partitions.push_back(GetExpression(child));
	}
	result->ignore_nulls = window.ignore_nulls;

	// Convert RANGE boundary expressions to ORDER +/- expressions.
	// Note that PRECEEDING and FOLLOWING refer to the sequential order in the frame,
	// not the natural ordering of the type. This means that the offset arithmetic must be reversed
	// for ORDER BY DESC.
	auto &config = DBConfig::GetConfig(context);
	auto range_sense = OrderType::INVALID;
	LogicalType start_type = LogicalType::BIGINT;
	if (window.start == WindowBoundary::EXPR_PRECEDING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = ResolveOrderType(config, window.orders[0].type);
		const auto name = (range_sense == OrderType::ASCENDING) ? "-" : "+";
		start_type = BindRangeExpression(context, name, window.start_expr, window.orders[0].expression);
	} else if (window.start == WindowBoundary::EXPR_FOLLOWING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = ResolveOrderType(config, window.orders[0].type);
		const auto name = (range_sense == OrderType::ASCENDING) ? "+" : "-";
		start_type = BindRangeExpression(context, name, window.start_expr, window.orders[0].expression);
	}

	LogicalType end_type = LogicalType::BIGINT;
	if (window.end == WindowBoundary::EXPR_PRECEDING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = ResolveOrderType(config, window.orders[0].type);
		const auto name = (range_sense == OrderType::ASCENDING) ? "-" : "+";
		end_type = BindRangeExpression(context, name, window.end_expr, window.orders[0].expression);
	} else if (window.end == WindowBoundary::EXPR_FOLLOWING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = ResolveOrderType(config, window.orders[0].type);
		const auto name = (range_sense == OrderType::ASCENDING) ? "+" : "-";
		end_type = BindRangeExpression(context, name, window.end_expr, window.orders[0].expression);
	}

	// Cast ORDER and boundary expressions to the same type
	if (range_sense != OrderType::INVALID) {
		D_ASSERT(window.orders.size() == 1);

		auto &order_expr = window.orders[0].expression;
		D_ASSERT(order_expr.get());
		D_ASSERT(order_expr->expression_class == ExpressionClass::BOUND_EXPRESSION);
		auto &bound_order = (BoundExpression &)*order_expr;
		auto order_type = bound_order.expr->return_type;
		if (window.start_expr) {
			order_type = LogicalType::MaxLogicalType(order_type, start_type);
		}
		if (window.end_expr) {
			order_type = LogicalType::MaxLogicalType(order_type, end_type);
		}

		// Cast all three to match
		bound_order.expr = BoundCastExpression::AddCastToType(context, std::move(bound_order.expr), order_type);
		start_type = end_type = order_type;
	}

	for (auto &order : window.orders) {
		auto type = ResolveOrderType(config, order.type);
		auto null_order = ResolveNullOrder(config, order.null_order);
		auto expression = GetExpression(order.expression);
		result->orders.emplace_back(type, null_order, std::move(expression));
	}

	result->filter_expr = CastWindowExpression(window.filter_expr, LogicalType::BOOLEAN);

	result->start_expr = CastWindowExpression(window.start_expr, start_type);
	result->end_expr = CastWindowExpression(window.end_expr, end_type);
	result->offset_expr = CastWindowExpression(window.offset_expr, LogicalType::BIGINT);
	result->default_expr = CastWindowExpression(window.default_expr, result->return_type);
	result->start = window.start;
	result->end = window.end;

	// create a BoundColumnRef that references this entry
	auto colref = make_unique<BoundColumnRefExpression>(std::move(name), result->return_type,
	                                                    ColumnBinding(node.window_index, node.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.windows.push_back(std::move(result));
	return BindResult(std::move(colref));
}

} // namespace duckdb
