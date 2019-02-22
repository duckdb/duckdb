#include "planner/expression_binder/update_binder.hpp"

using namespace duckdb;
using namespace std;

UpdateBinder::UpdateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult UpdateBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult(move(expr), "aggregate functions are not allowed in UPDATE");
	case ExpressionClass::WINDOW:
		return BindResult(move(expr), "window functions are not allowed in UPDATE");
	case ExpressionClass::SUBQUERY:
		return BindResult(move(expr), "subquery is not allowed in UPDATE (yet...)");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRefExpression(move(expr), depth);
	case ExpressionClass::FUNCTION:
		return BindFunctionExpression(move(expr), depth);
	default:
		return BindChildren(move(expr), depth);
	}
}
