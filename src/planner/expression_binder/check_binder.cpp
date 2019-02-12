#include "planner/expression_binder/check_binder.hpp"

using namespace duckdb;
using namespace std;


CheckBinder::CheckBinder(Binder &binder, ClientContext &context) : 
	ExpressionBinder(binder, context) {
	
}

BindResult CheckBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	switch(expr->GetExpressionClass()) {
		case ExpressionClass::AGGREGATE:
			return BindResult(move(expr), "aggregate functions are not allowed in check constraints");
		case ExpressionClass::WINDOW:
			return BindResult(move(expr), "window functions are not allowed in check constraints");
		case ExpressionClass::SUBQUERY:
			return BindResult(move(expr), "cannot use subquery in check constraint");
		case ExpressionClass::COLUMN_REF:
			return BindColumnRefExpression(move(expr), depth);
		case ExpressionClass::FUNCTION:
			return BindFunctionExpression(move(expr), depth);
		default:
			return BindChildren(move(expr), depth);
	}
}
