#include "duckdb/planner/expression_binder/update_binder.hpp"

namespace duckdb {

UpdateBinder::UpdateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult UpdateBinder::BindExpression(const ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult(BinderException::Unsupported(expr, "window functions are not allowed in UPDATE"));
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string UpdateBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in UPDATE";
}

} // namespace duckdb
