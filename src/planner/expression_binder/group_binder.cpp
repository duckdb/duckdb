#include "duckdb/planner/expression_binder/group_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, TableIndex group_index,
                         SelectBindState &bind_state)
    : ExpressionBinder(binder, context), node(node), bind_state(bind_state), group_index(group_index) {
}

BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindUnsupportedExpression(expr, depth, "GROUP BY clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindUnsupportedExpression(expr, depth, "GROUP BY clause cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string GroupBinder::UnsupportedAggregateMessage() {
	return "GROUP BY clause cannot contain aggregates!";
}

void GroupBinder::ReplaceSelectRef(SelectNode &node, SelectBindState &bind_state, ProjectionIndex group_index,
                                   unique_ptr<ParsedExpression> &expr_ptr) {
	auto &expr = *expr_ptr;
	idx_t select_list_idx;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// root is a column - check if it refers to an alias in the select list
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!IsPotentialAlias(colref)) {
			return;
		}
		auto &alias_name = colref.GetColumnName();
		auto entry = bind_state.alias_map.find(alias_name);
		if (entry == bind_state.alias_map.end()) {
			// no matching alias found
			return;
		}
		select_list_idx = entry->second;
		bind_state.group_alias_map[alias_name] = group_index;
		break;
	}
	case ExpressionClass::CONSTANT: {
		// root is a constant
		auto &constant = expr.Cast<ConstantExpression>();
		if (!constant.GetValue().type().IsIntegral()) {
			// non-integral expression, we just leave the constant here.
			return;
		}
		// INTEGER constant: we use the integer as an index into the select list (e.g. GROUP BY 1)
		auto index = (idx_t)constant.GetValue().GetValue<int64_t>();
		select_list_idx = index - 1;
		break;
	}
	default:
		return;
	}
	// this group entry directly refers to a select list expression
	// we need to switch them around - the select list expression needs to become a group
	// it will then instead refer to the group entry

	// check if we already have a reference to this group
	auto &used_aliases = bind_state.used_group_aliases;
	if (used_aliases.find(select_list_idx) != used_aliases.end()) {
		// the alias has already been bound to before!
		// this happens if we group on the same alias twice
		// e.g. GROUP BY k, k or GROUP BY 1, 1
		// in this case, we can just replace the grouping with a constant since the second grouping has no effect
		// (the constant grouping will be optimized out later)
		expr_ptr = make_uniq<ConstantExpression>(Value::INTEGER(42));
		return;
	}
	if (select_list_idx >= node.select_list.size()) {
		throw BinderException("GROUP BY term out of range - should be between 1 and %d", (int)node.select_list.size());
	}
	// move the expression that this refers to into the group expression
	expr_ptr = std::move(node.select_list[select_list_idx]);
	// now replace the original expression in the select list with a reference to this group
	bind_state.group_alias_map[to_string(select_list_idx)] = group_index;
	node.select_list[select_list_idx] = make_uniq<ColumnRefExpression>(to_string(select_list_idx));
	// insert into the set of used aliases
	used_aliases.insert(select_list_idx);
}

bool GroupBinder::TryResolveAliasReference(ColumnRefExpression &colref, idx_t depth, bool root_expression,
                                           BindResult &result, unique_ptr<ParsedExpression> &expr_ptr) {
	// try to resolve alias references in GROUP
	// failed to bind the column and the node is the root expression with depth = 0
	// check if refers to an alias in the select clause

	auto &alias_name = colref.GetColumnName();
	auto entry = bind_state.alias_map.find(alias_name);
	if (entry == bind_state.alias_map.end()) {
		// no matching alias found
		return false;
	}
	result = BindResult(BinderException(
	    colref, "Alias with name \"%s\" exists, but aliases cannot be used as part of an expression in the GROUP BY",
	    alias_name));
	return true;
}

} // namespace duckdb
