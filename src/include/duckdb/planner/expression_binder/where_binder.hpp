//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/where_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

namespace duckdb {

class ColumnAliasLookup;
class ColumnAliasBinder;

//! The WHERE binder is responsible for binding an expression within the WHERE clause of a SQL statement
class WhereBinder : public ExpressionBinder {
public:
	WhereBinder(Binder &binder, ClientContext &context);
	WhereBinder(Binder &binder, ClientContext &context, const case_insensitive_map_t<idx_t> &alias_map,
	            const BoundSelectNode &node);

protected:
	WhereBinder(Binder &binder, ClientContext &context, unique_ptr<ColumnAliasLookup> column_alias_lookup,
	            unique_ptr<ColumnAliasBinder> column_alias_binder);

	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	BindResult BindColumnRef(ColumnRefExpression &expr, idx_t depth, bool root_expression);

	string UnsupportedAggregateMessage() override;

private:
	//! filled for SELECT, null for DROP, UPDATE, etc.
	unique_ptr<ColumnAliasLookup> column_alias_lookup;
	//! filled for SELECT, null for DROP, UPDATE, etc.
	unique_ptr<ColumnAliasBinder> column_alias_binder;
};

} // namespace duckdb
