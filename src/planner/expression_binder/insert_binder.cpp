#include "duckdb/planner/expression_binder/insert_binder.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class Binder;
class ClientContext;

InsertBinder::InsertBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult InsertBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult(BinderException::Unsupported(expr, "DEFAULT is not allowed here!"));
	case ExpressionClass::WINDOW:
		return BindResult(BinderException::Unsupported(expr, "INSERT statement cannot contain window functions!"));
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string InsertBinder::UnsupportedAggregateMessage() {
	return "INSERT statement cannot contain aggregates!";
}

} // namespace duckdb
