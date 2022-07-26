#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

ColumnAliasBinder::ColumnAliasBinder(BoundSelectNode &node, const case_insensitive_map_t<idx_t> &alias_map)
    : node(node), alias_map(alias_map), in_alias(false) {
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

	if (in_alias) {
		return BindResult("Cannot resolve self-referential alias");
	}

	// found an alias: bind the alias expression
	auto expression = node.original_expressions[alias_entry->second]->Copy();
	in_alias = true;
	auto result = enclosing_binder.BindExpression(&expression, depth, root_expression);
	in_alias = false;
	return result;
}

} // namespace duckdb
