//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/constant_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! The Constant binder can bind ONLY constant foldable expressions (i.e. no subqueries, column refs, etc)
class ConstantBinder : public ExpressionBinder {
public:
	ConstantBinder(Binder &binder, ClientContext &context, string clause);

	//! The location where this binder is used, used for error messages
	string clause;

protected:
	BindResult BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
