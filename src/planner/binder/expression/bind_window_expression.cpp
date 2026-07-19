#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder/base_select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

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
	if (bound->GetReturnType() == LogicalType::SQLNULL) {
		throw BinderException(error_context, "Window RANGE expressions cannot be NULL");
	}
	children.emplace_back(std::move(bound));

	ErrorData error;
	FunctionBinder function_binder(context);
	auto function = function_binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(name),
	                                                   std::move(children), error, true);
	if (!function) {
		error.Throw();
	}
	// +/- can be applied to non-scalar types,
	// so we can't rely on function binding to catch all problems.
	if (!IsRangeType(function->GetReturnType())) {
		throw BinderException(error_context, "Invalid type for Window RANGE expression");
	}
	bound = std::move(function);
	return bound->GetReturnType();
}

BindResult BaseSelectBinder::BindWindowExpression(WindowExpression &window, idx_t depth) {
	QueryErrorContext error_context(window.GetQueryLocation());

	//	Check for macros pretending to be aggregates
	EntryLookupInfo function_lookup(CatalogType::SCALAR_FUNCTION_ENTRY, QualifiedName(window.FunctionName()),
	                                error_context);
	auto entry = GetCatalogEntry(window.GetQualifiedName().Catalog(), window.GetQualifiedName().Schema(),
	                             function_lookup, OnEntryNotFound::RETURN_NULL);
	if (entry && entry->type == CatalogType::MACRO_ENTRY) {
		auto macro_expr = window.Copy();
		auto macro = make_uniq<FunctionExpression>(window.GetQualifiedName(), std::move(window.GetArgumentsMutable()),
		                                           std::move(window.FilterMutable()), nullptr, window.Distinct());
		return BindMacro(*macro, entry->Cast<ScalarMacroCatalogEntry>(), depth, macro_expr);
	}

	auto name = window.GetAlias();

	if (inside_try) {
		throw BinderException("window functions are not allowed in try");
	}
	if (inside_aggregate) {
		throw BinderException(window, "aggregate function calls cannot contain window function calls");
	}
	if (inside_window) {
		throw BinderException(window, "window function calls cannot be nested");
	}
	if (depth > 0) {
		throw BinderException(window, "correlated columns in window functions not supported");
	}

	//  Look up the aggregate function in the catalog
	if (!entry ||
	    (entry->type != CatalogType::AGGREGATE_FUNCTION_ENTRY && entry->type != CatalogType::WINDOW_FUNCTION_ENTRY)) {
		//	Not an aggregate or window function: Look it up to generate error
		Catalog::GetEntry<AggregateFunctionCatalogEntry>(context,
		                                                 QualifiedName(window.GetQualifiedName().Catalog(),
		                                                               window.GetQualifiedName().Schema(),
		                                                               window.FunctionName()),
		                                                 error_context);
	}

	// If we have range expressions, then only one order by clause is allowed.
	const auto is_range = (window.WindowStart() == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                       window.WindowStart() == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	                       window.WindowEnd() == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                       window.WindowEnd() == WindowBoundary::EXPR_FOLLOWING_RANGE);
	if (is_range && window.OrderBy().size() != 1) {
		throw BinderException(error_context, "RANGE frames must have only one ORDER BY expression");
	}
	// bind inside the children of the window function
	// we set the inside_window flag to true to prevent binding nested window functions
	inside_window = true;
	ErrorData error;
	for (auto &child : window.GetArgumentsMutable()) {
		BindChild(child.GetExpressionMutable(), depth, error);
	}
	for (auto &child : window.PartitionsMutable()) {
		BindChild(child, depth, error);
	}
	for (auto &order : window.OrderByMutable()) {
		BindChild(order.expression, depth, error);

		//	If the frame is a RANGE frame and the type is a time,
		//	then we have to convert the time to a timestamp to avoid wrapping.
		if (!is_range || error.HasError()) {
			continue;
		}
		auto &order_expr = order.expression;
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		const auto type_id = bound_order->GetReturnType().id();
		if (type_id == LogicalTypeId::TIME || type_id == LogicalTypeId::TIME_TZ) {
			//	Convert to time + epoch and rebind
			unique_ptr<ParsedExpression> epoch = make_uniq<ConstantExpression>(Value::DATE(date_t::epoch()));
			BindChild(epoch, depth, error);
			BindRangeExpression(context, "+", order.expression, epoch);
		}
	}
	BindChild(window.FilterMutable(), depth, error);
	BindChild(window.StartExprMutable(), depth, error);
	BindChild(window.EndExprMutable(), depth, error);

	for (auto &order : window.ArgOrdersMutable()) {
		BindChild(order.expression, depth, error);
	}

	inside_window = false;
	if (error.HasError()) {
		// failed to bind children of window function
		return BindResult(std::move(error));
	}

	//	Restore any collation expressions
	for (auto &order : window.ArgOrdersMutable()) {
		auto &order_expr = order.expression;
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		ExpressionBinder::PushCollation(context, bound_order, bound_order->GetReturnType());
	}
	for (auto &part_expr : window.PartitionsMutable()) {
		auto &bound_partition = BoundExpression::GetExpression(*part_expr);
		ExpressionBinder::PushCollation(context, bound_partition, bound_partition->GetReturnType());
	}
	for (auto &order : window.OrderByMutable()) {
		auto &order_expr = order.expression;
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		ExpressionBinder::PushCollation(context, bound_order, bound_order->GetReturnType());
	}

	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	arguments.reserve(window.GetArguments().size());
	for (auto &arg : window.GetArgumentsMutable()) {
		auto &bound_arg = BoundExpression::GetExpression(*arg.GetExpressionMutable());

		// legacy function calls cannot have named arguments, so we ignore the names of the arguments during binding
		// and pass them all positionally. We do alias them by their name though, so that alias-capturing functions
		// (e.g. struct_pack) still work and so that re-serializing to the old format can match arguments by name.
		// Only override the alias when the argument actually carries a name, otherwise we would clobber the
		// display alias the binding assigned (e.g. clearing a column reference's name to its raw binding).
		if (!arg.GetName().empty()) {
			bound_arg->SetAlias(arg.GetName());
		}

		if (window.IsLegacyFunctionCall()) {
			arguments.emplace_back(Identifier(), std::move(bound_arg));
		} else {
			arguments.emplace_back(arg.GetName(), std::move(bound_arg));
		}
	}

	//  Determine the function type.
	LogicalType sql_type;
	unique_ptr<BoundAggregateFunction> aggregate;
	unique_ptr<BoundWindowFunction> window_func;
	unique_ptr<FunctionData> bind_info;
	vector<unique_ptr<Expression>> children;

	if (entry->type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		auto &func = entry->Cast<AggregateFunctionCatalogEntry>();

		if (window.HasIgnoreNulls()) {
			throw BinderException(error_context, "RESPECT/IGNORE NULLS is not supported for windowed aggregates");
		}

		// bind the aggregate
		FunctionBinder function_binder(binder);
		auto window_bound_aggregate = function_binder.BindAggregateFunction(func, std::move(arguments), error, nullptr);
		// No function found, throw an error
		if (!window_bound_aggregate) {
			error.AddQueryLocation(window);
			error.Throw();
		}

		// create the aggregate
		aggregate = make_uniq<BoundAggregateFunction>(window_bound_aggregate->Function());
		bind_info = std::move(window_bound_aggregate->BindInfoMutable());
		children = std::move(window_bound_aggregate->GetChildrenMutable());
		sql_type = window_bound_aggregate->GetReturnType();

	} else {
		auto &func = entry->Cast<WindowFunctionCatalogEntry>();

		FunctionBinder function_binder(binder);
		auto window_bound_function = function_binder.BindWindowFunction(
		    func, std::move(arguments), error, window.OrderByMutable(), window.ArgOrdersMutable());

		if (!window_bound_function) {
			error.AddQueryLocation(window);
			error.Throw();
		}

		// TODO: Move this into the binder
		auto &bound_function = *window_bound_function->WindowFunction();
		if (window.Distinct() && !bound_function.CanDistinct()) {
			throw BinderException(error_context, "DISTINCT is not implemented for the window function \"%s\"",
			                      window.FunctionName());
		}
		if (window.Filter() && !bound_function.CanFilter()) {
			throw BinderException(error_context, "FILTER is not implemented for the window function \"%s\"",
			                      window.FunctionName());
		}
		if (!window.ArgOrders().empty() && !bound_function.CanOrderBy()) {
			throw BinderException(error_context, "ORDER BY is not supported for the window function \"%s\"",
			                      window.FunctionName());
		}
		if (window.WindowExclude() != WindowExcludeMode::NO_OTHER && !window.ArgOrders().empty() &&
		    !bound_function.CanExclude()) {
			throw BinderException(error_context, "EXCLUDE is not supported for the window function \"%s\"",
			                      window.FunctionName());
		}
		if (window.HasIgnoreNulls() && !bound_function.CanIgnoreNulls()) {
			throw BinderException(error_context, "RESPECT/IGNORE NULLS is not supported for the window function \"%s\"",
			                      window.FunctionName());
		}

		window_func = std::move(window_bound_function->WindowFunctionMutable());
		bind_info = std::move(window_bound_function->BindInfoMutable());
		children = std::move(window_bound_function->GetChildrenMutable());
		sql_type = window_bound_function->GetReturnType();
	}

	auto result =
	    make_uniq<BoundWindowExpression>(sql_type, std::move(aggregate), std::move(window_func), std::move(bind_info));
	result->GetChildrenMutable() = std::move(children);
	for (auto &child : window.PartitionsMutable()) {
		result->PartitionsMutable().push_back(GetExpression(child));
	}
	result->IgnoreNullsMutable() = window.IgnoreNulls();
	result->DistinctMutable() = window.Distinct();

	// Convert RANGE boundary expressions to ORDER +/- expressions.
	// Note that PRECEDING and FOLLOWING refer to the sequential order in the frame,
	// not the natural ordering of the type. This means that the offset arithmetic must be reversed
	// for ORDER BY DESC.
	auto &config = DBConfig::GetConfig(context);
	auto range_sense = OrderType::INVALID;
	LogicalType start_type = LogicalType::BIGINT;
	if (window.WindowStart() == WindowBoundary::EXPR_PRECEDING_RANGE) {
		D_ASSERT(window.OrderBy().size() == 1);
		range_sense = config.ResolveOrder(context, window.OrderByMutable()[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "-" : "+";
		start_type =
		    BindRangeExpression(context, range_name, window.StartExprMutable(), window.OrderByMutable()[0].expression);

	} else if (window.WindowStart() == WindowBoundary::EXPR_FOLLOWING_RANGE) {
		D_ASSERT(window.OrderBy().size() == 1);
		range_sense = config.ResolveOrder(context, window.OrderByMutable()[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "+" : "-";
		start_type =
		    BindRangeExpression(context, range_name, window.StartExprMutable(), window.OrderByMutable()[0].expression);
	}

	LogicalType end_type = LogicalType::BIGINT;
	if (window.WindowEnd() == WindowBoundary::EXPR_PRECEDING_RANGE) {
		D_ASSERT(window.OrderBy().size() == 1);
		range_sense = config.ResolveOrder(context, window.OrderByMutable()[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "-" : "+";
		end_type =
		    BindRangeExpression(context, range_name, window.EndExprMutable(), window.OrderByMutable()[0].expression);

	} else if (window.WindowEnd() == WindowBoundary::EXPR_FOLLOWING_RANGE) {
		D_ASSERT(window.OrderBy().size() == 1);
		range_sense = config.ResolveOrder(context, window.OrderByMutable()[0].type);
		const auto range_name = (range_sense == OrderType::ASCENDING) ? "+" : "-";
		end_type =
		    BindRangeExpression(context, range_name, window.EndExprMutable(), window.OrderByMutable()[0].expression);
	}

	// Cast ORDER and boundary expressions to the same type
	if (range_sense != OrderType::INVALID) {
		D_ASSERT(window.OrderBy().size() == 1);

		auto &order_expr = window.OrderByMutable()[0].expression;
		D_ASSERT(order_expr.get());
		D_ASSERT(order_expr->GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
		auto &bound_order = BoundExpression::GetExpression(*order_expr);
		auto order_type = bound_order->GetReturnType();
		if (window.StartExpr()) {
			order_type = LogicalType::MaxLogicalType(context, order_type, start_type);
		}
		if (window.EndExpr()) {
			order_type = LogicalType::MaxLogicalType(context, order_type, end_type);
		}

		// Cast all three to match
		bound_order = BoundCastExpression::AddCastToType(context, std::move(bound_order), order_type);
		start_type = end_type = order_type;
	}

	for (auto &order : window.OrderByMutable()) {
		auto type = config.ResolveOrder(context, order.type);
		auto null_order = config.ResolveNullOrder(context, type, order.null_order);
		auto expression = GetExpression(order.expression);
		result->OrderByMutable().emplace_back(type, null_order, std::move(expression));
	}

	// Argument orders are just like arguments, not frames
	for (auto &order : window.ArgOrdersMutable()) {
		auto type = config.ResolveOrder(context, order.type);
		auto null_order = config.ResolveNullOrder(context, type, order.null_order);
		auto expression = GetExpression(order.expression);
		result->ArgOrdersMutable().emplace_back(type, null_order, std::move(expression));
	}

	result->FilterMutable() = CastWindowExpression(window.FilterMutable(), LogicalType::BOOLEAN);
	result->StartExprMutable() = CastWindowExpression(window.StartExprMutable(), start_type);
	result->EndExprMutable() = CastWindowExpression(window.EndExprMutable(), end_type);
	result->WindowStartMutable() = window.WindowStart();
	result->WindowEndMutable() = window.WindowEnd();
	result->WindowExcludeMutable() = window.WindowExclude();

	// move the WINDOW expression into the set of bound windows
	auto &window_type = result->GetReturnType();
	auto window_idx = ColumnBinding::PushExpression(node.windows, std::move(result));
	auto colref =
	    make_uniq<BoundColumnRefExpression>(name, window_type, ColumnBinding(node.window_index, window_idx), depth);
	return BindResult(std::move(colref));
}

} // namespace duckdb
