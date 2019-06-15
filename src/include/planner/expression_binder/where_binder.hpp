//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/where_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The WHERE binder is responsible for binding an expression within the WHERE clause of a SQL statement
class WhereBinder : public ExpressionBinder {
public:
	WhereBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(ParsedExpression &expr, index_t depth, bool root_expression = false) override;
};

} // namespace duckdb
