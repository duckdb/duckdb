#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : BaseSelectBinder(binder, context, node, info) {
}

bool SelectBinder::TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, BindResult &result) {
	// must be a qualified alias.<name>
	if (!ExpressionBinder::IsPotentialAlias(colref)) {
		return false;
	}

	const auto &alias_name = colref.column_names.back();
	auto entry = node.bind_state.alias_map.find(alias_name);
	if (entry == node.bind_state.alias_map.end()) {
		throw BinderException(colref, "alias.%s referenced, but no such alias exists in the SELECT list", alias_name);
	}

	auto alias_index = entry->second;
	// Simple way to prevent circular aliasing (`SELECT alias.y as x, alias.x as y;`)
	if (alias_index >= node.bound_column_count) {
		throw BinderException(colref, "alias.%s references an alias defined after the current expression", alias_name);
	}

	if (node.bind_state.AliasHasSubquery(alias_index)) {
		throw BinderException(colref,
		                      "Alias \"%s\" referenced in a SELECT clause - but the expression has a subquery. This is "
		                      "not yet supported.",
		                      alias_name);
	}
	auto copied_unbound = node.bind_state.BindAlias(alias_index);
	result = BindExpression(copied_unbound, depth, false);
	return true;
}

unique_ptr<ParsedExpression> SelectBinder::GetSQLValueFunction(const string &column_name) {
	auto alias_entry = node.bind_state.alias_map.find(column_name);
	if (alias_entry != node.bind_state.alias_map.end()) {
		// don't replace SQL value functions if they are in the alias map
		return nullptr;
	}
	return ExpressionBinder::GetSQLValueFunction(column_name);
}

} // namespace duckdb
