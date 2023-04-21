#include "duckdb/planner/expression_binder/update_binder.hpp"

namespace duckdb {

UpdateBinder::UpdateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult UpdateBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in UPDATE");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string UpdateBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in UPDATE";
}

} // namespace duckdb
