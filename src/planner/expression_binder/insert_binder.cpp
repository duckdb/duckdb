#include "duckdb/planner/expression_binder/insert_binder.hpp"

#include "duckdb/planner/expression/bound_default_expression.hpp"

using namespace duckdb;
using namespace std;

InsertBinder::InsertBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult InsertBinder::BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("DEFAULT is not allowed here!");
	case ExpressionClass::WINDOW:
		return BindResult("INSERT statement cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string InsertBinder::UnsupportedAggregateMessage() {
	return "INSERT statement cannot contain aggregates!";
}
