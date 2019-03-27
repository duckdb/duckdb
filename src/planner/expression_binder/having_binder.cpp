#include "planner/expression_binder/having_binder.hpp"

#include "main/client_context.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"

using namespace duckdb;
using namespace std;

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                           expression_map_t<uint32_t> &group_map, unordered_map<string, uint32_t> &group_alias_map)
    : SelectBinder(binder, context, node, group_map, group_alias_map) {
}

BindResult HavingBinder::BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression) {
	// check if the expression binds to one of the groups
	auto group_binding = TryBindGroup(expr, depth);
	if (group_binding) {
		return BindResult(move(group_binding));
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("HAVING clause cannot contain window functions!");
	case ExpressionClass::AGGREGATE:
		return BindAggregate((AggregateExpression&) expr, depth);
	case ExpressionClass::COLUMN_REF:
		return BindResult(StringUtil::Format("column %s must appear in the GROUP BY clause or be used in an aggregate function",
		                       expr.ToString().c_str()));
	default:
		return ExpressionBinder::BindExpression(expr, depth);
	}
}
