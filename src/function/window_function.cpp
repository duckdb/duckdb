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
	schema_name = base.schema_name;
	catalog_name = base.catalog_name;
	arguments = base.GetArguments();
	return_type = base.GetReturnType();
	callbacks = base.GetCallbacks();
	properties = base.GetProperties();
	function_info = base.GetFunctionInfo();
}

bool BoundWindowFunction::operator==(const BoundWindowFunction &rhs) const {
	return window_enum == rhs.window_enum && arguments == rhs.arguments && return_type == rhs.return_type;
}

bool BoundWindowFunction::operator!=(const BoundWindowFunction &rhs) const {
	return !(*this == rhs);
}

} // namespace duckdb
