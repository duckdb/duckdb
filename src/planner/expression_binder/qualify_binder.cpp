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

static bool IsExplicitAliasPrefix(const ColumnRefExpression &colref) {
	return colref.IsQualified() && colref.column_names.size() == 2 &&
	       StringUtil::CIEquals(colref.GetTableName(), "alias");
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
	// Inside a window's children a bare reference must bind to the FROM column: alias bodies
	// for window aliases cannot legally be expanded into another OVER clause. The explicit
	// `alias.X` form is exempt — the user has named the alias scope intentionally.
	bool allow_alias = !inside_window || IsExplicitAliasPrefix(colref);
	if (allow_alias && column_alias_binder.DoesColumnAliasExist(colref)) {
		return nullptr;
	}
	return qualified_colref;
}

BindResult QualifyBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	// At top-level QUALIFY scope, aliases take priority over base columns when the names
	// collide. The alias binder's visited-set guards against self-referential expansion
	// (e.g. `lead(n) OVER() AS n`), so the inner `n` falls through to a column lookup.
	// Inside a window's children we keep the original "column first, alias fallback" order
	// to avoid expanding an alias whose body is itself a window into another OVER clause.
	// The explicit `alias.X` form is exempt — the user has named the alias scope.
	auto &colref = expr_ptr->Cast<ColumnRefExpression>();
	bool prefer_alias_first = !inside_window || IsExplicitAliasPrefix(colref);

	if (prefer_alias_first && column_alias_binder.DoesColumnAliasExist(colref)) {
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

	if (!prefer_alias_first && column_alias_binder.DoesColumnAliasExist(colref)) {
		BindResult alias_result;
		auto found_alias = column_alias_binder.BindAlias(*this, expr_ptr, depth, root_expression, alias_result);
		if (found_alias) {
			return alias_result;
		}
	}

	auto expr_string = expr_ptr->Cast<ColumnRefExpression>().ToString();
	return BindResult(BinderException(
	    *expr_ptr, "Referenced column %s not found in FROM clause and can't find in alias map.", expr_string));
}

} // namespace duckdb
