#include "duckdb/planner/expression_binder/having_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                           const case_insensitive_map_t<idx_t> &alias_map, BoundGroupInformation &info)
    : SelectBinder(binder, context, node, alias_map, info), column_alias_projection_binder(node.projection_index) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult HavingBinder::BindColumnRef(ColumnRefExpression &expr, idx_t depth, bool root_expression) {
	auto alias_index = column_alias_lookup.TryBindAlias(expr);
	if (alias_index != DConstants::INVALID_INDEX) {
		return BindResult(column_alias_projection_binder.ResolveAliasWithProjection((ParsedExpression &)expr, alias_index));
	}

	return BindResult(StringUtil::Format(
	    "column %s must appear in the GROUP BY clause or be used in an aggregate function", expr.ToString()));
}

BindResult HavingBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("HAVING clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef((ColumnRefExpression &)expr, depth, root_expression);
	default:
		return duckdb::SelectBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb
