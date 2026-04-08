#include "duckdb/planner/expression_binder/aggregate_binder.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class Binder;
class ClientContext;

AggregateBinder::AggregateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult AggregateBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		throw BinderException::Unsupported(expr, "aggregate function calls cannot contain window function calls");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string AggregateBinder::UnsupportedAggregateMessage() {
	return "aggregate function calls cannot be nested";
}
} // namespace duckdb
