//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/check_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "planner/expression_binder.hpp"

namespace duckdb {
//! The CHECK binder is responsible for binding an expression within a CHECK constraint
class CheckBinder : public ExpressionBinder {
public:
	CheckBinder(Binder &binder, ClientContext &context, string table, vector<ColumnDefinition> &columns);

	string table;
	vector<ColumnDefinition> &columns;

protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;

	BindResult BindCheckColumn(ColumnRefExpression &expr);
};

} // namespace duckdb
