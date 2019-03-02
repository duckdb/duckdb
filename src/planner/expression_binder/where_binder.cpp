#include "planner/expression_binder/where_binder.hpp"

using namespace duckdb;
using namespace std;

WhereBinder::WhereBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult WhereBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult(move(expr), "WHERE clause cannot contain aggregates!");
	case ExpressionClass::SUBQUERY:
		return BindSubqueryExpression(move(expr), depth);
	case ExpressionClass::WINDOW:
		return BindResult(move(expr), "WHERE clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRefExpression(move(expr), depth);
	case ExpressionClass::FUNCTION:
		return BindFunctionExpression(move(expr), depth);
	default:
		return BindChildren(move(expr), depth);
	}
}
