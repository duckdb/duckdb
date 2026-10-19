#include "duckdb/planner/expression_binder/qualify_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

QualifyBinder::QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node)
    : BaseSelectBinder(binder, context, node), column_alias_binder(node.bind_state) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult QualifyBinder::BindColumnRef(const ParsedExpression &expr, idx_t depth, bool root_expression) {
	auto result = duckdb::BaseSelectBinder::BindColumnRef(expr, depth, root_expression);
	if (!result.HasError()) {
		return result;
	}

	// Keep the original column reference's string to return a meaningful error message.
	auto &col_ref = expr.Cast<ColumnRefExpression>();
	auto expr_string = col_ref.ToString();

	// Try to bind as an alias.
	BindResult alias_result;
	auto found_alias = column_alias_binder.BindAlias(*this, col_ref, depth, root_expression, alias_result);
	if (found_alias) {
		return alias_result;
	}

	return BindResult(BinderException(
	    expr, "Referenced column %s not found in FROM clause and can't find in alias map.", expr_string));
}

bool QualifyBinder::ClaimsAlias(const ColumnRefExpression &colref) {
	return column_alias_binder.DoesColumnAliasExist(colref);
}

} // namespace duckdb
