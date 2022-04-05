#include "duckdb/planner/expression_binder/group_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, BoundSelectNode &bound_node,
                         const case_insensitive_map_t<idx_t> &alias_map, idx_t group_index)
    : ExpressionBinder(binder, context), column_alias_lookup(alias_map), column_alias_binder(bound_node), node(node),
      group_index(group_index) {
}

BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	if (root_expression && depth == 0) {
		switch (expr.expression_class) {
		case ExpressionClass::COLUMN_REF:
			return BindColumnRef((ColumnRefExpression &)expr);
		case ExpressionClass::CONSTANT:
			return BindConstant((ConstantExpression &)expr);
		default:
			break;
		}
	}
	switch (expr.expression_class) {
	case ExpressionClass::DEFAULT:
		return BindResult("GROUP BY clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("GROUP BY clause cannot contain window functions!");
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
		return BindResult(make_unique<BoundConstantExpression>(Value::INTEGER(42)));
	}
	if (entry >= node.select_list.size()) {
		throw BinderException("GROUP BY term out of range - should be between 1 and %d", (int)node.select_list.size());
	}
	auto select_entry = column_alias_binder.ResolveAliasByDuplicatingParsedTarget(entry);
	// we replace the root expression, also replace the unbound expression
	unbound_expression = select_entry->Copy();
	// bind the expression that this refers to
	auto binding = Bind(select_entry, nullptr, false);
	// insert into the set of used aliases
	used_aliases.insert(entry);
	return BindResult(move(binding));
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

BindResult GroupBinder::BindColumnRef(ColumnRefExpression &colref) {
	// first fall through to an underlying table
	auto result = ExpressionBinder::BindExpression(colref, 0);
	if (!result.HasError()) {
		return result;
	}

	// no luck? try an alias
	auto alias_index = column_alias_lookup.TryBindAlias(colref);
	if (alias_index == DConstants::INVALID_INDEX) {
		// no alias, abort
		return BindResult(StringUtil::Format("%s, and GROUP BY \"%s\" couldn\'t be found in the FROM clause!",
		                                     result.error, colref.ToString()));
	}

	// TODO - check group_alias_map in master

	// found the alias, off we go
	return BindResult(BindSelectRef(alias_index));
}

} // namespace duckdb
