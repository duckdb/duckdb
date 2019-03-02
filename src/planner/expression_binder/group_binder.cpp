#include "planner/expression_binder/group_binder.hpp"

#include "parser/expression/bound_columnref_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node/select_node.hpp"

using namespace duckdb;
using namespace std;

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, unordered_map<string, uint32_t> &alias_map, unordered_map<string, uint32_t>& group_alias_map)
    : SelectNodeBinder(binder, context, node), alias_map(alias_map), group_alias_map(group_alias_map) {
}

BindResult GroupBinder::BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::AGGREGATE:
		return BindResult(move(expr), "GROUP BY clause cannot contain aggregates!");
	case ExpressionClass::WINDOW:
		return BindResult(move(expr), "GROUP clause cannot contain window functions!");
	case ExpressionClass::SUBQUERY:
		return BindSubqueryExpression(move(expr), depth);
	case ExpressionClass::FUNCTION:
		return BindFunctionExpression(move(expr), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(move(expr), depth, root_expression);
	case ExpressionClass::CONSTANT:
		return BindConstant(move(expr), depth, root_expression);
	default:
		return BindChildren(move(expr), depth);
	}
}

BindResult GroupBinder::BindSelectRef(uint32_t entry) {
	if (used_aliases.find(entry) != used_aliases.end()) {
		// the alias has already been bound to before!
		// this happens if we group on the same alias twice
		// e.g. GROUP BY k, k or GROUP BY 1, 1
		// in this case, we can just replace the grouping with a constant since the second grouping has no effect
		// (the constant grouping will be optimized out later)
		return BindResult(make_unique<ConstantExpression>(42));
	}
	if (entry >= node.select_list.size()) {
		throw BinderException("GROUP BY term out of range - should be between 1 and %d", (int)node.select_list.size());
	}
	// we replace the root expression, also replace the unbound expression
	unbound_expression = node.select_list[entry]->Copy();
	// move the expression that this refers to here and bind it
	auto result = move(node.select_list[entry]);
	BindAndResolveType(&result, false);
	// now replace the original expression in the select list with a reference to this group
	node.select_list[entry] = make_unique<BoundColumnRefExpression>(
		*result, result->return_type, ColumnBinding(node.binding.group_index, bind_index), 0);
	// insert into the set of used aliases
	used_aliases.insert(entry);
	return BindResult(move(result));
}

BindResult GroupBinder::BindConstant(unique_ptr<Expression> expr, uint32_t depth, bool root_expression) {
	assert(expr->type == ExpressionType::VALUE_CONSTANT);
	if (root_expression && depth == 0) {
		// constant as root expression
		auto &constant = (ConstantExpression&) *expr;
		if (!TypeIsIntegral(constant.value.type)) {
			// non-integral expression, we just leave the constant here.
			return BindResult(move(expr));
		}
		// INTEGER constant: we use the integer as an index into the select list (e.g. GROUP BY 1)
		auto index = constant.value.GetNumericValue();
		return BindSelectRef(index - 1);
	}
	return BindResult(move(expr));
}

BindResult GroupBinder::BindColumnRef(unique_ptr<Expression> expr, uint32_t depth, bool root_expression) {
	assert(expr->type == ExpressionType::COLUMN_REF);
	// columns in GROUP BY clauses:
	// FIRST refer to the original tables, and
	// THEN if no match is found refer to aliases in the SELECT list
	// THEN if no match is found, refer to outer queries

	// first try to bind to the base columns (original tables)
	auto result = ExpressionBinder::BindColumnRefExpression(move(expr), depth);
	if (result.HasError() && root_expression && depth == 0) {
		// failed to bind the column and the node is the root expression with depth = 0
		// check if refers to an alias in the select clause
		auto &colref = (ColumnRefExpression&) *result.expression;
		auto alias_name = colref.column_name;
		if (!colref.table_name.empty()) {
			// explicit table name: not an alias reference
			return result;
		}
		auto entry = alias_map.find(alias_name);
		if (entry == alias_map.end()) {
			// no matching alias found
			return result;
		}
		result = BindResult(BindSelectRef(entry->second));
		if (!result.HasError()) {
			group_alias_map[alias_name] = bind_index;
		}
	}
	return result;
}