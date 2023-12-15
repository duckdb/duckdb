//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/group_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class ConstantExpression;
class ColumnRefExpression;

//! The GROUP binder is responsible for binding expressions in the GROUP BY clause
class GroupBinder : public ExpressionBinder {
public:
	GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, idx_t group_index,
	            case_insensitive_map_t<idx_t> &alias_map, case_insensitive_map_t<idx_t> &group_alias_map);

	//! The unbound root expression
	unique_ptr<ParsedExpression> unbound_expression;
	//! The group index currently being bound
	idx_t bind_index;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;

	string UnsupportedAggregateMessage() override;

	BindResult BindSelectRef(idx_t entry);
	BindResult BindColumnRef(ColumnRefExpression &expr);
	BindResult BindConstant(ConstantExpression &expr);

	SelectNode &node;
	case_insensitive_map_t<idx_t> &alias_map;
	case_insensitive_map_t<idx_t> &group_alias_map;
	unordered_set<idx_t> used_aliases;

	idx_t group_index;
};

} // namespace duckdb
