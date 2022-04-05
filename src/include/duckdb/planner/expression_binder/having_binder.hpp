//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/having_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

namespace duckdb {

//! The HAVING binder is responsible for binding an expression within the HAVING clause of a SQL statement
class HavingBinder : public SelectBinder {
public:
	HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
	             const case_insensitive_map_t<idx_t> &alias_map, BoundGroupInformation &info);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;
	BindResult BindColumnRef(ColumnRefExpression &expr, idx_t depth, bool root_expression) override;

protected:
	ColumnAliasProjectionBinder column_alias_projection_binder;
};

} // namespace duckdb
