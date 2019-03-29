//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/index_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The INDEX binder is responsible for binding an expression within an Index statement
class IndexBinder : public ExpressionBinder {
public:
	IndexBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;
};

} // namespace duckdb
