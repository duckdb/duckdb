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

//! The WHERE binder is responsible for binding an expression within the WHERE clause of a SQL statement
class WhereBinder : public ExpressionBinder {
public:
	WhereBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
