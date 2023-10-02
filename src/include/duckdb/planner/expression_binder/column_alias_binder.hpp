//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/column_alias_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class BoundSelectNode;
class ColumnRefExpression;

//! A helper binder for WhereBinder and HavingBinder which support alias as a columnref.
class ColumnAliasBinder {
public:
	ColumnAliasBinder(BoundSelectNode &node, const case_insensitive_map_t<idx_t> &alias_map);

	BindResult BindAlias(ExpressionBinder &enclosing_binder, ColumnRefExpression &expr, idx_t depth,
	                     bool root_expression);

private:
	BoundSelectNode &node;
	const case_insensitive_map_t<idx_t> &alias_map;
	unordered_set<idx_t> visited_select_indexes;
};

} // namespace duckdb
