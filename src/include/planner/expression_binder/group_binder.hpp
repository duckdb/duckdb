//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/group_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The GROUP binder is responsible for binding expressions in the GROUP BY clause
class GroupBinder : public SelectNodeBinder {
public:
	GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, unordered_map<string, uint32_t> &alias_map, unordered_map<string, uint32_t>& group_alias_map);

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression = false) override;

	//! The unbound root expression
	unique_ptr<Expression> unbound_expression;
	//! The group index currently being bound
	uint32_t bind_index;
protected:
	BindResult BindSelectRef(uint32_t entry);
	BindResult BindColumnRef(unique_ptr<Expression> expr, uint32_t depth, bool root_expression);
	BindResult BindConstant(unique_ptr<Expression> expr, uint32_t depth, bool root_expression);

	unordered_map<string, uint32_t> &alias_map;
	unordered_map<string, uint32_t>& group_alias_map;
	unordered_set<uint32_t> used_aliases;
};

} // namespace duckdb
