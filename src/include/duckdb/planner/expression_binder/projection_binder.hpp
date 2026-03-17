//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/projection_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

class ColumnAliasBinder;

//! The Projection binder
class ProjectionBinder : public ExpressionBinder {
public:
	ProjectionBinder(Binder &binder, ClientContext &context, idx_t proj_index,
	                 vector<unique_ptr<Expression>> &proj_expressions, string clause);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);

private:
	idx_t proj_index;
	vector<unique_ptr<Expression>> &proj_expressions;
	string clause;
};

} // namespace duckdb
