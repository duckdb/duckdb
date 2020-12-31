#include "duckdb/planner/expression_binder/where_binder.hpp"

namespace duckdb {

WhereBinder::WhereBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult WhereBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("WHERE clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("WHERE clause cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string WhereBinder::UnsupportedAggregateMessage() {
	return "WHERE clause cannot contain aggregates!";
}

} // namespace duckdb
