//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/constant_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The Constant binder can bind ONLY constant foldable expressions (i.e. no subqueries, column refs, etc)
class ConstantBinder : public ExpressionBinder {
public:
	ConstantBinder(Binder &binder, ClientContext &context, string clause);

	//! The location where this binder is used, used for error messages
	string clause;

protected:
	BindResult BindExpression(ParsedExpression &expr, index_t depth, bool root_expression = false) override;
};

} // namespace duckdb
