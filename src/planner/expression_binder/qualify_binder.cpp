#include "duckdb/planner/expression_binder/qualify_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

QualifyBinder::QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : BaseSelectBinder(binder, context, node, info), column_alias_binder(node.bind_state) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult QualifyBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto result = duckdb::BaseSelectBinder::BindColumnRef(expr_ptr, depth, root_expression);
	if (!result.HasError()) {
		return result;
	}

	// Keep the original column reference's string to return a meaningful error message.
	auto expr_string = expr_ptr->Cast<ColumnRefExpression>().ToString();

	// Try to bind as an alias.
	BindResult alias_result;
	auto found_alias = column_alias_binder.BindAlias(*this, expr_ptr, depth, root_expression, alias_result);
	if (found_alias) {
		return alias_result;
	}

	return BindResult(
	    StringUtil::Format("Referenced column %s not found in FROM clause and can't find in alias map.", expr_string));
}

} // namespace duckdb
