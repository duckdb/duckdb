#include "duckdb/planner/expression_binder/alter_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

AlterBinder::AlterBinder(Binder &binder, ClientContext &context, TableCatalogEntry &table,
                         vector<LogicalIndex> &bound_columns, LogicalType target_type)
    : ExpressionBinder(binder, context), table(table), bound_columns(bound_columns) {
	this->target_type = std::move(target_type);
}

BindResult AlterBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in alter statement");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in alter statement");
	case ExpressionClass::COLUMN_REF:
		return BindColumn(expr.Cast<ColumnRefExpression>());
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string AlterBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in alter statement";
}

BindResult AlterBinder::BindColumn(ColumnRefExpression &colref) {
	if (colref.column_names.size() > 1) {
		return BindQualifiedColumnName(colref, table.name);
	}
	auto idx = table.GetColumnIndex(colref.column_names[0], true);
	if (!idx.IsValid()) {
		throw BinderException("Table does not contain column %s referenced in alter statement!",
		                      colref.column_names[0]);
	}
	if (table.GetColumn(idx).Generated()) {
		throw BinderException("Using generated columns in alter statement not supported");
	}
	bound_columns.push_back(idx);
	return BindResult(make_uniq<BoundReferenceExpression>(table.GetColumn(idx).Type(), bound_columns.size() - 1));
}

} // namespace duckdb
