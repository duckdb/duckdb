#include "duckdb/planner/expression_binder/lateral_binder.hpp"

namespace duckdb {

LateralBinder::LateralBinder(Binder &binder, ClientContext &context)
    : ExpressionBinder(binder, context) {
}

BindResult LateralBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("LATERAL join cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("LATERAL join cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string LateralBinder::UnsupportedAggregateMessage() {
	return "LATERAL join cannot contain aggregates!";
}

} // namespace duckdb
