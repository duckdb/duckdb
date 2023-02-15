#include "duckdb/planner/expression_binder/index_binder.hpp"

namespace duckdb {

IndexBinder::IndexBinder(Binder &binder, ClientContext &context, TableCatalogEntry *table)
    : ExpressionBinder(binder, context), table(table) {
}

BindResult IndexBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in index expressions");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in index expressions");
	case ExpressionClass::COLUMN_REF:
		if (table) {
			// WAL replay
			// we assume that the parsed expressions have qualified column names
			// and that the columns exist in the table
			auto &col_ref = (ColumnRefExpression &)expr;
			auto col_idx = table->GetColumnIndex(col_ref.column_names.back());
			auto col_type = table->GetColumn(col_idx).GetType();

			if (ref_expr_indexes.find(col_idx.index) == ref_expr_indexes.end()) {
				ref_expr_indexes[col_idx.index] = ref_expr_indexes.size();
			}

			auto bound_ref_expr =
			    make_unique<BoundReferenceExpression>(col_ref.alias, col_type, ref_expr_indexes[col_idx.index]);
			return BindResult(std::move(bound_ref_expr));
		}
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string IndexBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in index expressions";
}

} // namespace duckdb
