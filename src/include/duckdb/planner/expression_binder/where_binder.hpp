//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/where_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class ColumnAliasBinder;

//! The WHERE binder is responsible for binding an expression within the WHERE clause of a SQL statement
class WhereBinder : public ExpressionBinder {
public:
	WhereBinder(Binder &binder, ClientContext &context, optional_ptr<ColumnAliasBinder> column_alias_binder = nullptr);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);

	optional_ptr<ColumnAliasBinder> column_alias_binder;
};

} // namespace duckdb
