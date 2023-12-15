#include "duckdb/planner/expression_binder/insert_binder.hpp"

#include "duckdb/planner/expression/bound_default_expression.hpp"

namespace duckdb {

InsertBinder::InsertBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult InsertBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("DEFAULT is not allowed here!");
	case ExpressionClass::WINDOW:
		return BindResult("INSERT statement cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string InsertBinder::UnsupportedAggregateMessage() {
	return "INSERT statement cannot contain aggregates!";
}

} // namespace duckdb
