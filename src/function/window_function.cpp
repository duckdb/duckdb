#include "duckdb/function/window_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

unique_ptr<BoundWindowExpression> WindowFunction::Bind(ClientContext &context) const {
	vector<unique_ptr<Expression>> arguments;
	return Bind(context, std::move(arguments));
}

unique_ptr<BoundWindowExpression> WindowFunction::Bind(ClientContext &context,
                                                       vector<unique_ptr<Expression>> arguments) const {
	FunctionBinder func_binder(context);
	vector<OrderByNode> orders;
	vector<OrderByNode> arg_orders;

	return func_binder.BindWindowFunction(*this, std::move(arguments), orders, arg_orders);
}

BoundWindowFunction::BoundWindowFunction(const WindowFunction &base) : window_enum(base.window_enum) {
	name = base.name;
	schema_name = base.GetSchemaName();
	catalog_name = base.GetCatalogName();
	extra_info = base.extra_info;
	return_type = base.GetReturnType();
	callbacks = base.GetCallbacks();
	properties = base.GetProperties();
	function_info = base.GetFunctionInfo();

	// Try to default bind the function, to fill in any missing information in the BoundScalarFunction (e.g. from the
	// "bind" callback)
	for (auto &param : base.GetSignature().GetParameters()) {
		arguments.push_back(param.GetType());
	}
}

bool BoundWindowFunction::operator==(const BoundWindowFunction &rhs) const {
	return window_enum == rhs.window_enum && arguments == rhs.arguments && return_type == rhs.return_type;
}

bool BoundWindowFunction::operator!=(const BoundWindowFunction &rhs) const {
	return !(*this == rhs);
}

BindWindowFunctionInput::BindWindowFunctionInput(ClientContext &context_p, BoundWindowFunction &bound_function_p,
                                                 vector<unique_ptr<Expression>> &arguments_p,
                                                 const vector<Identifier> &argument_names_p, OptionalOrdering orders_p,
                                                 OptionalOrdering arg_orders_p)
    : BindFunctionInput(context_p, bound_function_p, arguments_p, &argument_names_p), bound_function(bound_function_p),
      orders(orders_p), arg_orders(arg_orders_p) {
}

BindWindowFunctionInput::BindWindowFunctionInput(ClientContext &context_p, BoundWindowFunction &bound_function_p,
                                                 vector<unique_ptr<Expression>> &arguments_p, OptionalOrdering orders_p,
                                                 OptionalOrdering arg_orders_p)
    : BindFunctionInput(context_p, bound_function_p, arguments_p, nullptr), bound_function(bound_function_p),
      orders(orders_p), arg_orders(arg_orders_p) {
}

} // namespace duckdb
