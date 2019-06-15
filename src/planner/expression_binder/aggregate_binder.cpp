#include "planner/expression_binder/aggregate_binder.hpp"

#include "planner/binder.hpp"

using namespace duckdb;
using namespace std;

AggregateBinder::AggregateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult AggregateBinder::BindExpression(ParsedExpression &expr, index_t depth, bool root_expression) {
	switch (expr.expression_class) {
	case ExpressionClass::AGGREGATE:
		throw ParserException("aggregate function calls cannot be nested");
	case ExpressionClass::WINDOW:
		throw ParserException("aggregate function calls cannot contain window function calls");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}
