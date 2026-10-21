#include "duckdb/planner/expression_binder/insert_binder.hpp"

#include "duckdb/planner/expression/bound_default_expression.hpp"

namespace duckdb {

InsertBinder::InsertBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult InsertBinder::BindExpression(const ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult(BinderException::Unsupported(expr, "DEFAULT is not allowed here!"));
	case ExpressionClass::WINDOW:
		return BindResult(BinderException::Unsupported(expr, "INSERT statement cannot contain window functions!"));
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string InsertBinder::UnsupportedAggregateMessage() {
	return "INSERT statement cannot contain aggregates!";
}

} // namespace duckdb
