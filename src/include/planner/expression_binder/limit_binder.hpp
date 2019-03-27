//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/limit_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The LIMIT binder is responsible for binding an expression within the LIMIT clause of a SQL statement
class LimitBinder : public ExpressionBinder {
public:
	LimitBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;
};

} // namespace duckdb
