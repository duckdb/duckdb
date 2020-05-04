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
//! The ALTER binder is responsible for binding an expression within alter statements
class AlterBinder : public ExpressionBinder {
public:
	AlterBinder(Binder &binder, ClientContext &context, string table, vector<ColumnDefinition> &columns,
	            vector<column_t> &bound_columns, SQLType target_type);

	string table;
	vector<ColumnDefinition> &columns;
	vector<column_t> &bound_columns;

protected:
	BindResult BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression = false) override;

	BindResult BindColumn(ColumnRefExpression &expr);

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
