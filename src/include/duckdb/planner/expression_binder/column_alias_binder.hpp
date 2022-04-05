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

//! A helper to allow binders to look up the index of a column alias.
class ColumnAliasLookup {
public:
	ColumnAliasLookup(const case_insensitive_map_t<idx_t> &alias_map);

	idx_t TryBindAlias(const ParsedExpression &expr);

protected:
	const case_insensitive_map_t<idx_t> &alias_map;
};

//! A helper to allow binders to resolve a column alias by duplicating the statement the alias refers to.
class ColumnAliasBinder {
public:
	ColumnAliasBinder(const BoundSelectNode &node);

	unique_ptr<ParsedExpression> ResolveAliasByDuplicatingParsedTarget(idx_t index);
	BindResult BindAliasByDuplicatingParsedTarget(ExpressionBinder *binder, const ParsedExpression &expr, idx_t index,
	                                              idx_t depth, bool root_expression);

protected:
	const BoundSelectNode &node;
};

//! A helper to allow binders to resolve a column alias by creating a projection reference to the statement the alias
//! refers to.
class ColumnAliasProjectionBinder {
public:
	ColumnAliasProjectionBinder(idx_t projection_index);

	unique_ptr<Expression> ResolveAliasWithProjection(const ParsedExpression &expr, idx_t index);

protected:
	idx_t projection_index;
};

} // namespace duckdb
