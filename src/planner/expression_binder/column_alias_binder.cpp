#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"

namespace duckdb {

ColumnAliasBinder::ColumnAliasBinder(SelectBindState &bind_state) : bind_state(bind_state), visited_select_indexes() {
}

bool ColumnAliasBinder::BindAlias(ExpressionBinder &enclosing_binder, unique_ptr<ParsedExpression> &expr_ptr,
                                  idx_t depth, bool root_expression, BindResult &result) {

	D_ASSERT(expr_ptr->GetExpressionClass() == ExpressionClass::COLUMN_REF);
	auto &expr = expr_ptr->Cast<ColumnRefExpression>();

	// Qualified columns cannot be aliases.
	if (expr.IsQualified()) {
		return false;
	}

	// We try to find the alias in the alias_map and return false, if no alias exists.
	auto alias_entry = bind_state.alias_map.find(expr.column_names[0]);
	if (alias_entry == bind_state.alias_map.end()) {
		return false;
	}

	if (visited_select_indexes.find(alias_entry->second) != visited_select_indexes.end()) {
		// self-referential alias cannot be resolved
		return false;
	}

	// We found an alias, so we copy the alias expression into this expression.
	auto original_expr = bind_state.BindAlias(alias_entry->second);
	expr_ptr = std::move(original_expr);
	visited_select_indexes.insert(alias_entry->second);

	result = enclosing_binder.BindExpression(expr_ptr, depth, root_expression);
	visited_select_indexes.erase(alias_entry->second);
	return true;
}

} // namespace duckdb
