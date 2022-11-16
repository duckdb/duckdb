//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/lateral_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class ColumnAliasBinder;

//! The LATERAL binder is responsible for binding an expression within a LATERAL join
class LateralBinder : public ExpressionBinder {
public:
	LateralBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
