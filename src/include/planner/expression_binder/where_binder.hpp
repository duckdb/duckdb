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

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth) override;
};

} // namespace duckdb
