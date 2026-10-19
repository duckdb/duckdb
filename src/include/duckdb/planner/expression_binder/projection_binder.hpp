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
	ProjectionBinder(Binder &binder, ClientContext &context, TableIndex proj_index,
	                 vector<unique_ptr<Expression>> &proj_expressions, string clause);

protected:
	BindResult BindExpression(const ParsedExpression &expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

	BindResult BindColumnRef(const ParsedExpression &expr, idx_t depth, bool root_expression);

private:
	TableIndex proj_index;
	vector<unique_ptr<Expression>> &proj_expressions;
	string clause;
	bool in_child_projection = false;
};

} // namespace duckdb
