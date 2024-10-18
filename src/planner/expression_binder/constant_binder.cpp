#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

ConstantBinder::ConstantBinder(Binder &binder, ClientContext &context, string clause)
    : ExpressionBinder(binder, context), clause(std::move(clause)) {
}

BindResult ConstantBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			auto value_function = GetSQLValueFunction(colref.GetColumnName());
			if (value_function) {
				expr_ptr = std::move(value_function);
				return BindExpression(expr_ptr, depth, root_expression);
			}
		}
		return BindUnsupportedExpression(expr, depth, clause + " cannot contain column names");
	}
	case ExpressionClass::SUBQUERY:
		throw BinderException(clause + " cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindUnsupportedExpression(expr, depth, clause + " cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindUnsupportedExpression(expr, depth, clause + " cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string ConstantBinder::UnsupportedAggregateMessage() {
	return clause + " cannot contain aggregates!";
}

} // namespace duckdb
