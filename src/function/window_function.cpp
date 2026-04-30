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

BoundWindowFunction::BoundWindowFunction(const WindowFunction &base) : WindowFunction(base) {
}

} // namespace duckdb
