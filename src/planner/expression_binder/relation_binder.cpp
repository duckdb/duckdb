#include "duckdb/planner/expression_binder/relation_binder.hpp"

namespace duckdb {

RelationBinder::RelationBinder(Binder &binder, ClientContext &context, string op)
    : ExpressionBinder(binder, context), op(std::move(op)) {
}

BindResult RelationBinder::BindExpression(const ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult(BinderException::Unsupported(expr, "aggregate functions are not allowed in " + op));
	case ExpressionClass::DEFAULT:
		return BindResult(BinderException::Unsupported(expr, op + " cannot contain DEFAULT clause"));
	case ExpressionClass::SUBQUERY:
		return BindResult(BinderException::Unsupported(expr, "subqueries are not allowed in " + op));
	case ExpressionClass::WINDOW:
		return BindResult(BinderException::Unsupported(expr, "window functions are not allowed in " + op));
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string RelationBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in " + op;
}

} // namespace duckdb
