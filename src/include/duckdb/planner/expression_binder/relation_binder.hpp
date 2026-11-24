//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/relation_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! The relation binder binds expressions that may not contain aggregates, DEFAULT, subqueries or window functions
class RelationBinder : public ExpressionBinder {
public:
	RelationBinder(Binder &binder, ClientContext &context, string op);

	string op;

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;
};

} // namespace duckdb
