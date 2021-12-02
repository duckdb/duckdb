#include "duckdb/planner/expression_binder/qualify_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

QualifyBinder::QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                           unordered_map<string, idx_t> &alias_map)
    : SelectBinder(binder, context, node, info), column_alias_binder(node, alias_map) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult QualifyBinder::BindColumnRef(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = (ColumnRefExpression &)**expr_ptr;
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError()) {
		return result;
	}

	auto alias_result = column_alias_binder.BindAlias(*this, expr, depth, root_expression);
	if (!alias_result.HasError()) {
		return alias_result;
	}

	return result;
}

BindResult QualifyBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindWindow((WindowExpression &)expr, depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return duckdb::SelectBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb
