#include "duckdb/planner/expression_binder/aggregate_binder.hpp"

#include "duckdb/planner/binder.hpp"

namespace duckdb {

AggregateBinder::AggregateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult AggregateBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		throw ParserException("aggregate function calls cannot contain window function calls");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string AggregateBinder::UnsupportedAggregateMessage() {
	return "aggregate function calls cannot be nested";
}
} // namespace duckdb
