#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : BaseSelectBinder(binder, context, node, info) {
}

unique_ptr<Expression> SelectBinder::TryResolveAliasReference(const string &alias_name,
                                                              const ColumnRefExpression &column_ref_expr) {
	// resolve alias_ref(name) within SELECT list
	auto entry = node.bind_state.alias_map.find(alias_name);
	if (entry == node.bind_state.alias_map.end()) {
		throw BinderException(column_ref_expr, "alias_ref('%s') referenced, but no such alias exists in the SELECT list",
		                      alias_name);
	}
	auto alias_index = entry->second;

	// Simple way to prevent circular aliasing (`SELECT alias_ref('y') as x, alias_ref('x') as y;`)
	if (alias_index >= node.bound_column_count) {
		throw BinderException(column_ref_expr, "alias_ref('%s') references an alias defined after the current expression",
		                      alias_name);
	}

	// Restricting alias references to subqueries as we will need to define some caveats we want to enforce
	if (node.bind_state.AliasHasSubquery(alias_index)) {
		throw BinderException(column_ref_expr,
		                      "Alias \"%s\" referenced in a SELECT clause - but the expression has a subquery. This is "
		                      "not yet supported.",
		                      alias_name);
	}
	auto copied_unbound = node.bind_state.BindAlias(alias_index);
	return Bind(copied_unbound);
}

unique_ptr<ParsedExpression> SelectBinder::GetSQLValueFunction(const string &column_name) {
	auto alias_entry = node.bind_state.alias_map.find(column_name);
	if (alias_entry != node.bind_state.alias_map.end()) {
		// don't replace SQL value functions if they are in the alias map
		return nullptr;
	}
	return ExpressionBinder::GetSQLValueFunction(column_name);
}

BindResult SelectBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	// first try to bind the column reference regularly
	auto result = BaseSelectBinder::BindColumnRef(expr_ptr, depth, root_expression);
	if (!result.HasError()) {
		return result;
	}
	// binding failed
	// check in the alias map
	auto &colref = (expr_ptr.get())->Cast<ColumnRefExpression>();
	if (!colref.IsQualified()) {
		auto &bind_state = node.bind_state;
		auto alias_entry = node.bind_state.alias_map.find(colref.column_names[0]);
		if (alias_entry != node.bind_state.alias_map.end()) {
			// found entry!
			auto index = alias_entry->second;
			if (index >= node.bound_column_count) {
				throw BinderException("Column \"%s\" referenced that exists in the SELECT clause - but this column "
				                      "cannot be referenced before it is defined",
				                      colref.column_names[0]);
			}
			if (bind_state.AliasHasSubquery(index)) {
				throw BinderException("Alias \"%s\" referenced in a SELECT clause - but the expression has a subquery."
				                      " This is not yet supported.",
				                      colref.column_names[0]);
			}
			auto copied_expression = node.bind_state.BindAlias(index);
			result = BindExpression(copied_expression, depth, false);
			return result;
		}
	}
	// entry was not found in the alias map: return the original error
	return result;
}

bool SelectBinder::QualifyColumnAlias(const ColumnRefExpression &colref) {
	if (!colref.IsQualified()) {
		return node.bind_state.alias_map.find(colref.column_names[0]) != node.bind_state.alias_map.end();
	}
	return false;
}

} // namespace duckdb
