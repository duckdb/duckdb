#include "duckdb/planner/expression_binder/check_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

CheckBinder::CheckBinder(Binder &binder, ClientContext &context, string table_p, vector<ColumnDefinition> &columns,
                         unordered_set<column_t> &bound_columns)
    : ExpressionBinder(binder, context), table(move(table_p)), columns(columns), bound_columns(bound_columns) {
	target_type = LogicalType::INTEGER;
}

BindResult CheckBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in check constraints");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in check constraint");
	case ExpressionClass::COLUMN_REF:
		return BindCheckColumn((ColumnRefExpression &)expr);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string CheckBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in check constraints";
}

BindResult ExpressionBinder::BindQualifiedColumnName(ColumnRefExpression &colref, const string &table_name) {
	idx_t struct_start = 0;
	if (colref.column_names[0] == table_name) {
		struct_start++;
	}
	auto result = make_unique_base<ParsedExpression, ColumnRefExpression>(colref.column_names.back());
	for (idx_t i = struct_start; i + 1 < colref.column_names.size(); i++) {
		result = CreateStructExtract(move(result), colref.column_names[i]);
	}
	return BindExpression(&result, 0);
}

BindResult CheckBinder::BindCheckColumn(ColumnRefExpression &colref) {
	if (colref.column_names.size() > 1) {
		return BindQualifiedColumnName(colref, table);
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (colref.column_names[0] == col.Name()) {
			if (col.Generated()) {
				auto bound_expression = col.GeneratedExpression().Copy();
				return BindExpression(&bound_expression, 0, false);
			}
			bound_columns.insert(i);
			D_ASSERT(col.StorageOid() != DConstants::INVALID_INDEX);
			return BindResult(make_unique<BoundReferenceExpression>(col.Type(), col.StorageOid()));
		}
	}
	throw BinderException("Table does not contain column %s referenced in check constraint!", colref.column_names[0]);
}

} // namespace duckdb
