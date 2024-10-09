#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

WhereBinder::WhereBinder(Binder &binder, ClientContext &context, optional_ptr<ColumnAliasBinder> column_alias_binder)
    : ExpressionBinder(binder, context), column_alias_binder(column_alias_binder) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult WhereBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {

	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError() || !column_alias_binder) {
		return result;
	}

	BindResult alias_result;
	auto found_alias = column_alias_binder->BindAlias(*this, expr_ptr, depth, root_expression, alias_result);
	if (found_alias) {
		return alias_result;
	}

	return result;
}

BindResult WhereBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindUnsupportedExpression(expr, depth, "WHERE clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindUnsupportedExpression(expr, depth, "WHERE clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string WhereBinder::UnsupportedAggregateMessage() {
	return "WHERE clause cannot contain aggregates!";
}

bool WhereBinder::QualifyColumnAlias(const ColumnRefExpression &colref) {
	if (column_alias_binder) {
		return column_alias_binder->QualifyColumnAlias(colref);
	}
	return false;
}

} // namespace duckdb
