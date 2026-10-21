#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"

namespace duckdb {

ColumnAliasBinder::ColumnAliasBinder(SelectBindState &bind_state) : bind_state(bind_state), visited_select_indexes() {
}

unique_ptr<ParsedExpression> ColumnAliasBinder::ResolveAlias(const ColumnRefExpression &colref) {
	if (!ExpressionBinder::IsPotentialAlias(colref)) {
		return nullptr;
	}

	// We try to find the alias in the alias_map and return false, if no alias exists.
	auto alias_entry = bind_state.alias_map.find(colref.ColumnNames().back());
	if (alias_entry == bind_state.alias_map.end()) {
		return nullptr;
	}

	// We found an alias - bind it
	return bind_state.BindAlias(alias_entry->second);
}

bool ColumnAliasBinder::BindAlias(ExpressionBinder &enclosing_binder, const ColumnRefExpression &expr, idx_t depth,
                                  bool root_expression, BindResult &result) {
	if (!ExpressionBinder::IsPotentialAlias(expr)) {
		return false;
	}

	// We try to find the alias in the alias_map and return false, if no alias exists.
	auto alias_entry = bind_state.alias_map.find(expr.ColumnNames().back());
	if (alias_entry == bind_state.alias_map.end()) {
		return false;
	}

	if (visited_select_indexes.find(alias_entry->second) != visited_select_indexes.end()) {
		// self-referential alias cannot be resolved
		return false;
	}

	// We found an alias, so we bind a copy of the alias expression in place of this expression.
	auto alias_expr = bind_state.BindAlias(alias_entry->second);
	visited_select_indexes.insert(alias_entry->second);

	result = enclosing_binder.BindExpression(*alias_expr, depth, root_expression);
	visited_select_indexes.erase(alias_entry->second);
	return true;
}

bool ColumnAliasBinder::DoesColumnAliasExist(const ColumnRefExpression &colref) {
	if (!ExpressionBinder::IsPotentialAlias(colref)) {
		return false;
	}
	return bind_state.alias_map.find(colref.ColumnNames().back()) != bind_state.alias_map.end();
}

} // namespace duckdb
