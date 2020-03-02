#include "duckdb/planner/expression_binder/check_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

CheckBinder::CheckBinder(Binder &binder, ClientContext &context, string table, vector<ColumnDefinition> &columns,
                         unordered_set<column_t> &bound_columns)
    : ExpressionBinder(binder, context), table(table), columns(columns), bound_columns(bound_columns) {
	target_type = SQLType::INTEGER;
}

BindResult CheckBinder::BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in check constraints");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in check constraint");
	case ExpressionClass::COLUMN_REF:
		return BindCheckColumn((ColumnRefExpression &)expr);
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}

string CheckBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in check constraints";
}

BindResult CheckBinder::BindCheckColumn(ColumnRefExpression &colref) {
	if (!colref.table_name.empty() && colref.table_name != table) {
		throw BinderException("Cannot reference table %s from within check constraint for table %s!",
		                      colref.table_name.c_str(), table.c_str());
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		if (colref.column_name == columns[i].name) {
			bound_columns.insert(i);
			return BindResult(make_unique<BoundReferenceExpression>(GetInternalType(columns[i].type), i),
			                  columns[i].type);
		}
	}
	throw BinderException("Table does not contain column %s referenced in check constraint!",
	                      colref.column_name.c_str());
}
