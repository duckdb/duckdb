#include "duckdb/planner/expression_binder/having_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : SelectBinder(binder, context, node, info) {
	target_type = SQLType(SQLTypeId::BOOLEAN);
}

BindResult HavingBinder::BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) {
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("HAVING clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindResult(
		    StringUtil::Format("column %s must appear in the GROUP BY clause or be used in an aggregate function",
		                       expr.ToString().c_str()));
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}
