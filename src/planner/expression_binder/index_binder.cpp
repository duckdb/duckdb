#include "planner/expression_binder/index_binder.hpp"

using namespace duckdb;
using namespace std;

IndexBinder::IndexBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult IndexBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult(move(expr), "aggregate functions are not allowed in index expressions");
	case ExpressionClass::WINDOW:
		return BindResult(move(expr), "window functions are not allowed in index expressions");
	case ExpressionClass::SUBQUERY:
		return BindResult(move(expr), "cannot use subquery in index expressions");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRefExpression(move(expr), depth);
	case ExpressionClass::FUNCTION:
		return BindFunctionExpression(move(expr), depth);
	default:
		return BindChildren(move(expr), depth);
	}
}
