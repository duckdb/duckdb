#include "duckdb/planner/expression_binder/update_binder.hpp"

using namespace duckdb;
using namespace std;

UpdateBinder::UpdateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult UpdateBinder::BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in UPDATE");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string UpdateBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in UPDATE";
}
