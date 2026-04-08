#include "duckdb/planner/expression_binder/update_binder.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class Binder;
class ClientContext;

UpdateBinder::UpdateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult UpdateBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult(BinderException::Unsupported(expr, "window functions are not allowed in UPDATE"));
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string UpdateBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in UPDATE";
}

} // namespace duckdb
