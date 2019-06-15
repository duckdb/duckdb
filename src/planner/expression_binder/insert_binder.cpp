#include "planner/expression_binder/insert_binder.hpp"

#include "planner/expression/bound_default_expression.hpp"

using namespace duckdb;
using namespace std;

InsertBinder::InsertBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult InsertBinder::BindExpression(ParsedExpression &expr, index_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult("INSERT statement cannot contain aggregates!");
	case ExpressionClass::DEFAULT:
		if (!root_expression) {
			return BindResult("DEFAULT must be the root expression!");
		}
		return BindResult(make_unique<BoundDefaultExpression>(), SQLType());
	case ExpressionClass::WINDOW:
		return BindResult("INSERT statement cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}
