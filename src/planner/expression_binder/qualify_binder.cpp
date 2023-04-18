#include "duckdb/planner/expression_binder/qualify_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

QualifyBinder::QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                             case_insensitive_map_t<idx_t> &alias_map)
    : BaseSelectBinder(binder, context, node, info), column_alias_binder(node, alias_map) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult QualifyBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = expr_ptr->Cast<ColumnRefExpression>();
	auto result = duckdb::BaseSelectBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError()) {
		return result;
	}

	auto alias_result = column_alias_binder.BindAlias(*this, expr, depth, root_expression);
	if (!alias_result.HasError()) {
		return alias_result;
	}

	return BindResult(StringUtil::Format("Referenced column %s not found in FROM clause and can't find in alias map.",
	                                     expr.ToString()));
}

BindResult QualifyBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindWindow(expr.Cast<WindowExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return duckdb::BaseSelectBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb
