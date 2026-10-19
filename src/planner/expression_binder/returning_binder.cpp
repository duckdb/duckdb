#include "duckdb/planner/expression_binder/returning_binder.hpp"

#include "duckdb/planner/expression/bound_default_expression.hpp"

namespace duckdb {

ReturningBinder::ReturningBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult ReturningBinder::BindExpression(const ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		return BindResult(BinderException::Unsupported(expr, "SUBQUERY is not supported in returning statements"));
	case ExpressionClass::BOUND_SUBQUERY:
		return BindResult(
		    BinderException::Unsupported(expr, "BOUND SUBQUERY is not supported in returning statements"));
	case ExpressionClass::COLUMN_REF:
		return ExpressionBinder::BindExpression(expr, depth);
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

} // namespace duckdb
