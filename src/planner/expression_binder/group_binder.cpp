#include "planner/expression_binder/group_binder.hpp"

using namespace duckdb;
using namespace std;

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node)
    : SelectNodeBinder(binder, context, node) {
}

BindResult GroupBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult(move(expr), "GROUP BY clause cannot contain aggregates!");
	case ExpressionClass::WINDOW:
		return BindResult(move(expr), "GROUP clause cannot contain window functions!");
	case ExpressionClass::SUBQUERY:
		return BindSubqueryExpression(move(expr), depth);
	case ExpressionClass::FUNCTION:
		return BindFunctionExpression(move(expr), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnRefExpression(move(expr), depth);
	default:
		return BindChildren(move(expr), depth);
	}
}
