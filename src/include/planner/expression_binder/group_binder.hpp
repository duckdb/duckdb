//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/group_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "common/unordered_set.hpp"
#include "planner/expression_binder.hpp"

namespace duckdb {
class ConstantExpression;
class ColumnRefExpression;

//! The GROUP binder is responsible for binding expressions in the GROUP BY clause
class GroupBinder : public ExpressionBinder {
public:
	GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, index_t group_index,
	            unordered_map<string, index_t> &alias_map, unordered_map<string, index_t> &group_alias_map);

	//! The unbound root expression
	unique_ptr<ParsedExpression> unbound_expression;
	//! The group index currently being bound
	index_t bind_index;

protected:
	BindResult BindExpression(ParsedExpression &expr, index_t depth, bool root_expression) override;

	BindResult BindSelectRef(index_t entry);
	BindResult BindColumnRef(ColumnRefExpression &expr);
	BindResult BindConstant(ConstantExpression &expr);

	SelectNode &node;
	unordered_map<string, index_t> &alias_map;
	unordered_map<string, index_t> &group_alias_map;
	unordered_set<index_t> used_aliases;

	index_t group_index;
};

} // namespace duckdb
