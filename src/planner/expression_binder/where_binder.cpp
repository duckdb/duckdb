#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

namespace duckdb {

WhereBinder::WhereBinder(Binder &binder, ClientContext &context, unique_ptr<ColumnAliasLookup> column_alias_lookup,
                         unique_ptr<ColumnAliasBinder> column_alias_binder)
    : ExpressionBinder(binder, context), column_alias_lookup {move(column_alias_lookup)}, column_alias_binder {move(
                                                                                              column_alias_binder)} {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

WhereBinder::WhereBinder(Binder &binder, ClientContext &context) : WhereBinder(binder, context, nullptr, nullptr) {
}

WhereBinder::WhereBinder(Binder &binder, ClientContext &context, const case_insensitive_map_t<idx_t> &alias_map,
                         const BoundSelectNode &node)
    : WhereBinder(binder, context, make_unique<ColumnAliasLookup>(alias_map), make_unique<ColumnAliasBinder>(node)) {
}

BindResult WhereBinder::BindColumnRef(ColumnRefExpression &expr, idx_t depth, bool root_expression) {
	auto result = ExpressionBinder::BindExpression(expr, depth);
	if (!result.HasError()) {
		return result;
	}

	if (!column_alias_lookup || !column_alias_binder) {
		return result;
	}

	auto alias_index = column_alias_lookup->TryBindAlias(expr);
	if (alias_index != DConstants::INVALID_INDEX) {
		return column_alias_binder->BindAliasByDuplicatingParsedTarget(this, (ParsedExpression &)expr, alias_index,
		                                                               depth, root_expression);
	}
	return BindResult(StringUtil::Format("%s, and no existing column for \"%s\" found", result.error, expr.ToString()));
}

BindResult WhereBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("WHERE clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("WHERE clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef((ColumnRefExpression &)expr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string WhereBinder::UnsupportedAggregateMessage() {
	return "WHERE clause cannot contain aggregates!";
}

} // namespace duckdb
