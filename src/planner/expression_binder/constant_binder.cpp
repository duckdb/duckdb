#include "duckdb/planner/expression_binder/constant_binder.hpp"

using namespace duckdb;
using namespace std;

ConstantBinder::ConstantBinder(Binder &binder, ClientContext &context, string clause)
    : ExpressionBinder(binder, context), clause(clause) {
}

BindResult ConstantBinder::BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
		return BindResult(clause + " cannot contain column names");
	case ExpressionClass::SUBQUERY:
		return BindResult(clause + " cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindResult(clause + " cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult(clause + " cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string ConstantBinder::UnsupportedAggregateMessage() {
	return clause + "cannot contain aggregates!";
}
