//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/check_binder.hpp
//
//	
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {
	
//! The CHECK binder is responsible for binding an expression within a CHECK constraint
class CheckBinder : public ExpressionBinder {
public:
	CheckBinder(Binder &binder, ClientContext &context);

	BindResult BindExpression(unique_ptr<Expression> expr, uint32_t depth) override;
};

}
