#include "planner/expression_binder/check_binder.hpp"

#include "parser/expression/columnref_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace std;

CheckBinder::CheckBinder(Binder &binder, ClientContext &context, string table, vector<ColumnDefinition> &columns)
    : ExpressionBinder(binder, context), table(table), columns(columns) {
	target_type = SQLType(SQLTypeId::INTEGER);
}

BindResult CheckBinder::BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult("aggregate functions are not allowed in check constraints");
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

BindResult CheckBinder::BindCheckColumn(ColumnRefExpression &colref) {
	if (!colref.table_name.empty() && colref.table_name != table) {
		throw BinderException("Cannot reference table %s from within check constraint for table %s!",
		                      colref.table_name.c_str(), table.c_str());
	}
	for (uint64_t i = 0; i < columns.size(); i++) {
		assert(i <= numeric_limits<uint32_t>::max());

		if (colref.column_name == columns[i].name) {
			return BindResult(make_unique<BoundReferenceExpression>(GetInternalType(columns[i].type), (uint32_t)i),
			                  columns[i].type);
		}
	}
	throw BinderException("Table does not contain column %s referenced in check constraint!",
	                      colref.column_name.c_str());
}
