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

bool QualifyBinder::DoesColumnAliasExist(const ColumnRefExpression &colref) {
	return column_alias_binder.DoesColumnAliasExist(colref);
}

unique_ptr<ParsedExpression> QualifyBinder::QualifyColumnName(ColumnRefExpression &colref, ErrorData &error) {
	auto qualified_colref = ExpressionBinder::QualifyColumnName(colref, error);
	if (!qualified_colref) {
		return nullptr;
	}

	auto group_index = TryBindGroup(*qualified_colref);
	if (group_index.IsValid()) {
		return qualified_colref;
	}
	if (column_alias_binder.DoesColumnAliasExist(colref)) {
		return nullptr;
	}
	return qualified_colref;
}

BindResult QualifyBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	// First try alias resolution if the colref is a potential alias and the alias exists.
	// The alias binder's visited-set guards against self-referential expansion (e.g. `lead(n) OVER() AS n`),
	// in which case the inner `n` falls through to a regular column lookup.
	auto &colref = expr_ptr->Cast<ColumnRefExpression>();
	if (column_alias_binder.DoesColumnAliasExist(colref)) {
		BindResult alias_result;
		auto found_alias = column_alias_binder.BindAlias(*this, expr_ptr, depth, root_expression, alias_result);
		if (found_alias) {
			return alias_result;
		}
	}

	auto result = duckdb::BaseSelectBinder::BindColumnRef(expr_ptr, depth, root_expression);
	if (!result.HasError()) {
		return result;
	}

	auto expr_string = expr_ptr->Cast<ColumnRefExpression>().ToString();
	return BindResult(BinderException(
	    *expr_ptr, "Referenced column %s not found in FROM clause and can't find in alias map.", expr_string));
}

} // namespace duckdb
