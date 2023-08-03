#include "duckdb/planner/expression_binder/index_binder.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

IndexBinder::IndexBinder(Binder &binder, ClientContext &context, optional_ptr<TableCatalogEntry> table,
                         optional_ptr<CreateIndexInfo> info)
    : ExpressionBinder(binder, context), table(table), info(info) {
}

BindResult IndexBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in index expressions");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in index expressions");
	case ExpressionClass::COLUMN_REF: {
		if (table) {
			// WAL replay
			// we assume that the parsed expressions have qualified column names
			// and that the columns exist in the table
			auto &col_ref = expr.Cast<ColumnRefExpression>();
			auto col_idx = table->GetColumnIndex(col_ref.column_names.back());
			auto col_type = table->GetColumn(col_idx).GetType();

			// find the col_idx in the index.column_ids
			auto col_id_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < info->column_ids.size(); i++) {
				if (col_idx.index == info->column_ids[i]) {
					col_id_idx = i;
				}
			}

			if (col_id_idx == DConstants::INVALID_INDEX) {
				throw InternalException("failed to replay CREATE INDEX statement - column id not found");
			}
			return BindResult(
			    make_uniq<BoundColumnRefExpression>(col_ref.GetColumnName(), col_type, ColumnBinding(0, col_id_idx)));
		}
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string IndexBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in index expressions";
}

} // namespace duckdb
