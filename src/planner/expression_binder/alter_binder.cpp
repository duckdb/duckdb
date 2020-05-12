#include "duckdb/planner/expression_binder/alter_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace std;

namespace duckdb {

AlterBinder::AlterBinder(Binder &binder, ClientContext &context, string table, vector<ColumnDefinition> &columns,
                         vector<column_t> &bound_columns, SQLType target_type)
    : ExpressionBinder(binder, context), table(table), columns(columns), bound_columns(bound_columns) {
	this->target_type = target_type;
}

BindResult AlterBinder::BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in alter statement");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in alter statement");
	case ExpressionClass::COLUMN_REF:
		return BindColumn((ColumnRefExpression &)expr);
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string AlterBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in alter statement";
}

BindResult AlterBinder::BindColumn(ColumnRefExpression &colref) {
	if (!colref.table_name.empty() && colref.table_name != table) {
		throw BinderException("Cannot reference table %s from within alter statement for table %s!",
		                      colref.table_name.c_str(), table.c_str());
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		if (colref.column_name == columns[i].name) {
			bound_columns.push_back(i);
			return BindResult(
			    make_unique<BoundReferenceExpression>(GetInternalType(columns[i].type), bound_columns.size() - 1),
			    columns[i].type);
		}
	}
	throw BinderException("Table does not contain column %s referenced in alter statement!",
	                      colref.column_name.c_str());
}

} // namespace duckdb
