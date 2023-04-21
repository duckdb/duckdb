#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

ColumnAliasBinder::ColumnAliasBinder(BoundSelectNode &node, const case_insensitive_map_t<idx_t> &alias_map)
    : node(node), alias_map(alias_map), visited_select_indexes() {
}

BindResult ColumnAliasBinder::BindAlias(ExpressionBinder &enclosing_binder, ColumnRefExpression &expr, idx_t depth,
                                        bool root_expression) {
	if (expr.IsQualified()) {
		return BindResult(StringUtil::Format("Alias %s cannot be qualified.", expr.ToString()));
	}

	auto alias_entry = alias_map.find(expr.column_names[0]);
	if (alias_entry == alias_map.end()) {
		return BindResult(StringUtil::Format("Alias %s is not found.", expr.ToString()));
	}

	if (visited_select_indexes.find(alias_entry->second) != visited_select_indexes.end()) {
		return BindResult("Cannot resolve self-referential alias");
	}

	// found an alias: bind the alias expression
	auto expression = node.original_expressions[alias_entry->second]->Copy();
	visited_select_indexes.insert(alias_entry->second);

	// since the alias has been found, pass a depth of 0. See Issue 4978 (#16)
	// ColumnAliasBinders are only in Having, Qualify and Where Binders
	auto result = enclosing_binder.BindExpression(expression, 0, root_expression);
	visited_select_indexes.erase(alias_entry->second);
	return result;
}

} // namespace duckdb
