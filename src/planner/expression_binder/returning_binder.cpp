#include "duckdb/planner/expression_binder/returning_binder.hpp"

#include "duckdb/planner/expression/bound_default_expression.hpp"

namespace duckdb {

ReturningBinder::ReturningBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult ReturningBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	if (expr.GetName() == "rowid") {
		// We don't support rowid on inserts/updates or deletes. It's possible
		// the data still lives the transactional log and getting the true rowid is difficult.
		return BindResult("rowid is not supported in returning statements");
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		return BindResult("SUBQUERY is not supported in returning statements");
	case ExpressionClass::BOUND_SUBQUERY:
		return BindResult("BOUND SUBQUERY is not supported in returning statements");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb
