//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/projection_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ColumnAliasBinder;
class Binder;
class ClientContext;
class Expression;
class ParsedExpression;

//! The Projection binder
class ProjectionBinder : public ExpressionBinder {
public:
	ProjectionBinder(Binder &binder, ClientContext &context, TableIndex proj_index,
	                 vector<unique_ptr<Expression>> &proj_expressions, string clause);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);

private:
	TableIndex proj_index;
	vector<unique_ptr<Expression>> &proj_expressions;
	string clause;
};

} // namespace duckdb
