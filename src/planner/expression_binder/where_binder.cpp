#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

WhereBinder::WhereBinder(Binder &binder, ClientContext &context, optional_ptr<ColumnAliasBinder> column_alias_binder)
    : ExpressionBinder(binder, context), column_alias_binder(column_alias_binder) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult WhereBinder::BindExpression(const ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindUnsupportedExpression(expr, depth, "WHERE clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindUnsupportedExpression(expr, depth, "WHERE clause cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string WhereBinder::UnsupportedAggregateMessage() {
	return "WHERE clause cannot contain aggregates!";
}

bool WhereBinder::TryResolveAliasReference(const ColumnRefExpression &colref, idx_t depth, bool root_expression,
                                           BindResult &result) {
	if (!column_alias_binder) {
		return false;
	}
	return column_alias_binder->BindAlias(*this, colref, depth, root_expression, result);
}

bool WhereBinder::ClaimsAlias(const ColumnRefExpression &colref) {
	return column_alias_binder && column_alias_binder->DoesColumnAliasExist(colref);
}

} // namespace duckdb
