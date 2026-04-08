//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/returning_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class Binder;
class ClientContext;
class ParsedExpression;

//! The RETURNING binder is responsible for binding expressions within the RETURNING statement
class ReturningBinder : public ExpressionBinder {
public:
	ReturningBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;
};

} // namespace duckdb
