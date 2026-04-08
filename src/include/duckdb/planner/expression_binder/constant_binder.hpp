//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/constant_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class Binder;
class ClientContext;
class ParsedExpression;

//! The Constant binder can bind ONLY constant foldable expressions (i.e. no subqueries, column refs, etc)
class ConstantBinder : public ExpressionBinder {
public:
	ConstantBinder(Binder &binder, ClientContext &context, string clause);

	//! The location where this binder is used, used for error messages
	string clause;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
