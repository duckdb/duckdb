//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/update_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The UPDATE binder is responsible for binding an expression within an UPDATE statement
class UpdateBinder : public ExpressionBinder {
public:
	UpdateBinder(Binder &binder, ClientContext &context);

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth, bool root_expression = false) override;
};

} // namespace duckdb
