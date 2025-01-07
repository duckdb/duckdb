#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

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

static unique_ptr<Expression> GetExpression(unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return nullptr;
	}
	D_ASSERT(expr.get());
	D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	return std::move(BoundExpression::GetExpression(*expr));
}

static unique_ptr<Expression> CastWindowExpression(unique_ptr<ParsedExpression> &expr, const LogicalType &type) {
	if (!expr) {
		return nullptr;
	}
	D_ASSERT(expr.get());
	D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);

	auto &bound = BoundExpression::GetExpression(*expr);
	bound = BoundCastExpression::AddDefaultCastToType(std::move(bound), type);

	return std::move(bound);
}

static bool IsRangeType(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
		return true;
	default:
		return false;
	}
}

static LogicalType BindRangeExpression(ClientContext &context, const string &name, unique_ptr<ParsedExpression> &expr,
                                       unique_ptr<ParsedExpression> &order_expr) {

	vector<unique_ptr<Expression>> children;

	D_ASSERT(order_expr.get());
	D_ASSERT(order_expr->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	auto &bound_order = BoundExpression::GetExpression(*order_expr);
	children.emplace_back(bound_order->Copy());

	D_ASSERT(expr.get());
	D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	auto &bound = BoundExpression::GetExpression(*expr);
	QueryErrorContext error_context(bound->GetQueryLocation());
	if (bound->return_type == LogicalType::SQLNULL) {
		throw BinderException(error_context, "Window RANGE expressions cannot be NULL");
	}
	children.emplace_back(std::move(bound));

	ErrorData error;
	FunctionBinder function_binder(context);
	auto function = function_binder.BindScalarFunction(DEFAULT_SCHEMA, name, std::move(children), error, true);
	if (!function) {
		error.Throw();
	}
	// +/- can be applied to non-scalar types,
	// so we can't rely on function binding to catch all problems.
	if (!IsRangeType(function->return_type)) {
		throw BinderException(error_context, "Invalid type for Window RANGE expression");
	}
	bound = std::move(function);
	return bound->return_type;
}

BindResult BaseSelectBinder::BindWindow(WindowExpression &window, idx_t depth) {
	QueryErrorContext error_context(window.GetQueryLocation());
	//	Check for macros pretending to be aggregates
	auto entry = GetCatalogEntry(CatalogType::SCALAR_FUNCTION_ENTRY, window.catalog, window.schema,
	                             window.function_name, OnEntryNotFound::RETURN_NULL, error_context);
	if (window.GetExpressionType() == ExpressionType::WINDOW_AGGREGATE && entry &&
	    entry->type == CatalogType::MACRO_ENTRY) {
		auto macro = make_uniq<FunctionExpression>(window.catalog, window.schema, window.function_name,
		                                           std::move(window.children), std::move(window.filter_expr), nullptr,
		                                           window.distinct);
		auto macro_expr = window.Copy();
		return BindMacro(*macro, entry->Cast<ScalarMacroCatalogEntry>(), depth, macro_expr);
	}

	auto name = window.alias;

	if (inside_window) {
		throw BinderException(error_context, "window function calls cannot be nested");
	}
	if (depth > 0) {
		throw BinderException(error_context, "correlated columns in window functions not supported");
	}

	// If we have range expressions, then only one order by clause is allowed.
	const auto is_range =
	    (window.start == WindowBoundary::EXPR_PRECEDING_RANGE || window.start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	     window.end == WindowBoundary::EXPR_PRECEDING_RANGE || window.end == WindowBoundary::EXPR_FOLLOWING_RANGE);
	if (is_range && window.orders.size() != 1) {
		throw BinderException(error_context, "RANGE frames must have only one ORDER BY expression");
	}
	// bind inside the children of the window function
	// we set the inside_window flag to true to prevent binding nested window functions
	inside_window = true;
	ErrorData error;
	for (auto &child : window.children) {
		BindChild(child, depth, error);
	}
	for (auto &child : window.partitions) {
		BindChild(child, depth, error);
	}
	for (auto &order : window.orders) {
		BindChild(order.expression, depth, error);

		//	If the frame is a RANGE frame and the type is a time,
		//	then we have to convert the time to a timestamp to avoid wrapping.
		if (!is_range || error.HasError()) {
			continue;
		}
		auto &order_expr = order.expression;
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		const auto type_id = bound_order->return_type.id();
		if (type_id == LogicalTypeId::TIME || type_id == LogicalTypeId::TIME_TZ) {
			//	Convert to time + epoch and rebind
			unique_ptr<ParsedExpression> epoch = make_uniq<ConstantExpression>(Value::DATE(date_t::epoch()));
			BindChild(epoch, depth, error);
			BindRangeExpression(context, "+", order.expression, epoch);
		}
	}
	BindChild(window.filter_expr, depth, error);
	BindChild(window.start_expr, depth, error);
	BindChild(window.end_expr, depth, error);
	BindChild(window.offset_expr, depth, error);
	BindChild(window.default_expr, depth, error);

	for (auto &order : window.arg_orders) {
		BindChild(order.expression, depth, error);
	}

	inside_window = false;
	if (error.HasError()) {
		// failed to bind children of window function
		return BindResult(std::move(error));
	}

	//	Restore any collation expressions
	for (auto &order : window.arg_orders) {
		auto &order_expr = order.expression;
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		ExpressionBinder::PushCollation(context, bound_order, bound_order->return_type);
	}
	for (auto &part_expr : window.partitions) {
		auto &bound_partition = BoundExpression::GetExpression(*part_expr);
		ExpressionBinder::PushCollation(context, bound_partition, bound_partition->return_type);
	}
	for (auto &order : window.orders) {
		auto &order_expr = order.expression;
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		ExpressionBinder::PushCollation(context, bound_order, bound_order->return_type);
	}
	// successfully bound all children: create bound window function
	vector<LogicalType> types;
	vector<unique_ptr<Expression>> children;
	for (auto &child : window.children) {
		D_ASSERT(child.get());
		D_ASSERT(child->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
		auto &bound = BoundExpression::GetExpression(*child);
		// Add casts for positional arguments
		const auto argno = children.size();
		switch (window.GetExpressionType()) {
		case ExpressionType::WINDOW_NTILE:
			// ntile(bigint)
			if (argno == 0) {
				bound = BoundCastExpression::AddCastToType(context, std::move(bound), LogicalType::BIGINT);
			}
			break;
		case ExpressionType::WINDOW_NTH_VALUE:
			// nth_value(<expr>, index)
			if (argno == 1) {
				bound = BoundCastExpression::AddCastToType(context, std::move(bound), LogicalType::BIGINT);
			}
			break;
		default:
			break;
		}
		types.push_back(bound->return_type);
		children.push_back(std::move(bound));
	}
	//  Determine the function type.
	LogicalType sql_type;
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	if (window.GetExpressionType() == ExpressionType::WINDOW_AGGREGATE) {
		//  Look up the aggregate function in the catalog
		if (!entry || entry->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			//	Not an aggregate: Look it up to generate error
			Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, window.catalog, window.schema,
			                                                 window.function_name, error_context);
		}
		auto &func = entry->Cast<AggregateFunctionCatalogEntry>();
		D_ASSERT(func.type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

		// bind the aggregate
		ErrorData error_aggr;
		FunctionBinder function_binder(context);
		auto best_function = function_binder.BindFunction(func.name, func.functions, types, error_aggr);
		if (!best_function.IsValid()) {
			error_aggr.AddQueryLocation(window);
			error_aggr.Throw();
		}

		// found a matching function! bind it as an aggregate
		auto bound_function = func.functions.GetFunctionByOffset(best_function.GetIndex());
		auto window_bound_aggregate = function_binder.BindAggregateFunction(bound_function, std::move(children));
		// create the aggregate
		aggregate = make_uniq<AggregateFunction>(window_bound_aggregate->function);
		bind_info = std::move(window_bound_aggregate->bind_info);
		children = std::move(window_bound_aggregate->children);
		sql_type = window_bound_aggregate->return_type;

	} else {
		// fetch the child of the non-aggregate window function (if any)
		sql_type = ResolveWindowExpressionType(window.GetExpressionType(), types);
	}
	auto result = make_uniq<BoundWindowExpression>(window.GetExpressionType(), sql_type, std::move(aggregate),
	                                               std::move(bind_info));
	result->children = std::move(children);
	for (auto &child : window.partitions) {
		result->partitions.push_back(GetExpression(child));
	}
	result->ignore_nulls = window.ignore_nulls;
	result->distinct = window.distinct;

	// Convert RANGE boundary expressions to ORDER +/- expressions.
	// Note that PRECEEDING and FOLLOWING refer to the sequential order in the frame,
	// not the natural ordering of the type. This means that the offset arithmetic must be reversed
	// for ORDER BY DESC.
	auto &config = DBConfig::GetConfig(context);
	auto range_sense = OrderType::INVALID;
	LogicalType start_type = LogicalType::BIGINT;
	if (window.start == WindowBoundary::EXPR_PRECEDING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = config.ResolveOrder(window.orders[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "-" : "+";
		start_type = BindRangeExpression(context, range_name, window.start_expr, window.orders[0].expression);

	} else if (window.start == WindowBoundary::EXPR_FOLLOWING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = config.ResolveOrder(window.orders[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "+" : "-";
		start_type = BindRangeExpression(context, range_name, window.start_expr, window.orders[0].expression);
	}

	LogicalType end_type = LogicalType::BIGINT;
	if (window.end == WindowBoundary::EXPR_PRECEDING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = config.ResolveOrder(window.orders[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "-" : "+";
		end_type = BindRangeExpression(context, range_name, window.end_expr, window.orders[0].expression);

	} else if (window.end == WindowBoundary::EXPR_FOLLOWING_RANGE) {
		D_ASSERT(window.orders.size() == 1);
		range_sense = config.ResolveOrder(window.orders[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "+" : "-";
		end_type = BindRangeExpression(context, range_name, window.end_expr, window.orders[0].expression);
	}

	// Cast ORDER and boundary expressions to the same type
	if (range_sense != OrderType::INVALID) {
		D_ASSERT(window.orders.size() == 1);

		auto &order_expr = window.orders[0].expression;
		D_ASSERT(order_expr.get());
		D_ASSERT(order_expr->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		auto order_type = bound_order->return_type;
		if (window.start_expr) {
			order_type = LogicalType::MaxLogicalType(context, order_type, start_type);
		}
		if (window.end_expr) {
			order_type = LogicalType::MaxLogicalType(context, order_type, end_type);
		}

		// Cast all three to match
		bound_order = BoundCastExpression::AddCastToType(context, std::move(bound_order), order_type);
		start_type = end_type = order_type;
	}

	for (auto &order : window.orders) {
		auto type = config.ResolveOrder(order.type);
		auto null_order = config.ResolveNullOrder(type, order.null_order);
		auto expression = GetExpression(order.expression);
		result->orders.emplace_back(type, null_order, std::move(expression));
	}

	// Argument orders are just like arguments, not frames
	for (auto &order : window.arg_orders) {
		auto type = config.ResolveOrder(order.type);
		auto null_order = config.ResolveNullOrder(type, order.null_order);
		auto expression = GetExpression(order.expression);
		result->arg_orders.emplace_back(type, null_order, std::move(expression));
	}

	result->filter_expr = CastWindowExpression(window.filter_expr, LogicalType::BOOLEAN);
	result->start_expr = CastWindowExpression(window.start_expr, start_type);
	result->end_expr = CastWindowExpression(window.end_expr, end_type);
	result->offset_expr = CastWindowExpression(window.offset_expr, LogicalType::BIGINT);
	result->default_expr = CastWindowExpression(window.default_expr, result->return_type);
	result->start = window.start;
	result->end = window.end;
	result->exclude_clause = window.exclude_clause;

	// create a BoundColumnRef that references this entry
	auto colref = make_uniq<BoundColumnRefExpression>(std::move(name), result->return_type,
	                                                  ColumnBinding(node.window_index, node.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.windows.push_back(std::move(result));
	return BindResult(std::move(colref));
}

} // namespace duckdb
