#include "duckdb/planner/expression_binder/group_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, idx_t group_index,
                         SelectBindState &bind_state, case_insensitive_map_t<idx_t> &group_alias_map)
    : ExpressionBinder(binder, context), node(node), bind_state(bind_state), group_alias_map(group_alias_map),
      group_index(group_index) {
}

BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	if (root_expression && depth == 0) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::COLUMN_REF:
			return BindColumnRef(expr.Cast<ColumnRefExpression>());
		case ExpressionClass::CONSTANT:
			return BindConstant(expr.Cast<ConstantExpression>());
		case ExpressionClass::PARAMETER:
			throw ParameterNotAllowedException("Parameter not supported in GROUP BY clause");
		default:
			break;
		}
	}
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

BindResult GroupBinder::BindSelectRef(idx_t entry) {
	if (used_aliases.find(entry) != used_aliases.end()) {
		// the alias has already been bound to before!
		// this happens if we group on the same alias twice
		// e.g. GROUP BY k, k or GROUP BY 1, 1
		// in this case, we can just replace the grouping with a constant since the second grouping has no effect
		// (the constant grouping will be optimized out later)
		return BindResult(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	}
	if (entry >= node.select_list.size()) {
		throw BinderException("GROUP BY term out of range - should be between 1 and %d", (int)node.select_list.size());
	}
	// we replace the root expression, also replace the unbound expression
	unbound_expression = node.select_list[entry]->Copy();
	// move the expression that this refers to here and bind it
	auto select_entry = std::move(node.select_list[entry]);
	auto binding = Bind(select_entry, nullptr, false);
	// now replace the original expression in the select list with a reference to this group
	group_alias_map[to_string(entry)] = bind_index;
	node.select_list[entry] = make_uniq<ColumnRefExpression>(to_string(entry));
	// insert into the set of used aliases
	used_aliases.insert(entry);
	return BindResult(std::move(binding));
}

BindResult GroupBinder::BindConstant(ConstantExpression &constant) {
	// constant as root expression
	if (!constant.value.type().IsIntegral()) {
		// non-integral expression, we just leave the constant here.
		return ExpressionBinder::BindExpression(constant, 0);
	}
	// INTEGER constant: we use the integer as an index into the select list (e.g. GROUP BY 1)
	auto index = (idx_t)constant.value.GetValue<int64_t>();
	return BindSelectRef(index - 1);
}

bool GroupBinder::TryBindAlias(ColumnRefExpression &colref, bool root_expression, BindResult &result) {
	// failed to bind the column and the node is the root expression with depth = 0
	// check if refers to an alias in the select clause
	auto &alias_name = colref.GetColumnName();
	auto entry = bind_state.alias_map.find(alias_name);
	if (entry == bind_state.alias_map.end()) {
		// no matching alias found
		return false;
	}
	if (!root_expression) {
		result = BindResult(BinderException(
		    colref,
		    "Alias with name \"%s\" exists, but aliases cannot be used as part of an expression in the GROUP BY",
		    alias_name));
		return true;
	}
	result = BindResult(BindSelectRef(entry->second));
	if (!result.HasError()) {
		group_alias_map[alias_name] = bind_index;
	}
	return true;
}

BindResult GroupBinder::BindColumnRef(ColumnRefExpression &colref) {
	// columns in GROUP BY clauses:
	// FIRST refer to the original tables, and
	// THEN if no match is found refer to aliases in the SELECT list
	// THEN if no match is found, refer to outer queries

	// first try to bind to the base columns (original tables)
	return ExpressionBinder::BindExpression(colref, 0, true);
}

} // namespace duckdb
