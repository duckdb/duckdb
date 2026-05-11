#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : BaseSelectBinder(binder, context, node, info) {
}

bool SelectBinder::TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression,
                                            BindResult &result, unique_ptr<ParsedExpression> &expr_ptr) {
	// must be a qualified alias.<name>
	if (!ExpressionBinder::IsPotentialAlias(colref)) {
		return false;
	}

	const auto &alias_name = colref.column_names.back();
	auto entry = node.bind_state.alias_map.find(alias_name);
	if (entry == node.bind_state.alias_map.end()) {
		return false;
	}

	auto alias_index = entry->second;
	// Simple way to prevent circular aliasing (`SELECT alias.y as x, alias.x as y;`)
	if (alias_index >= node.bound_column_count) {
		throw BinderException("Column \"%s\" referenced that exists in the SELECT clause - but this column "
		                      "cannot be referenced before it is defined",
		                      colref.column_names.back());
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

} // namespace duckdb
