//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/alter_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class TableCatalogEntry;

//! The ALTER binder is responsible for binding an expression within alter statements
class AlterBinder : public ExpressionBinder {
public:
	AlterBinder(Binder &binder, ClientContext &context, TableCatalogEntry &table, vector<LogicalIndex> &bound_columns,
	            LogicalType target_type);

	TableCatalogEntry &table;
	vector<LogicalIndex> &bound_columns;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	BindResult BindColumn(ColumnRefExpression &expr);

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
