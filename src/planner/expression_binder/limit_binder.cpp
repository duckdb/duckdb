#include "planner/expression_binder/limit_binder.hpp"

using namespace duckdb;
using namespace std;

LimitBinder::LimitBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult LimitBinder::BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult("LIMIT clause cannot contain aggregates!");
	case ExpressionClass::COLUMN_REF:
		return BindResult("LIMIT clause cannot contain column names");
	case ExpressionClass::SUBQUERY:
		return BindResult("LIMIT clause cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindResult("LIMIT clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("LIMIT clause cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}
