//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/returning_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! The RETURNING binder is responsible for binding expressions within the RETURNING statement
class ReturningBinder : public ExpressionBinder {
public:
	ReturningBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	// check certain column ref Names to make sure they are supported in the returning statement
	// (i.e rowid)
	BindResult BindColumnRef(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth);
};

} // namespace duckdb
