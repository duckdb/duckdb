//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/lateral_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ColumnAliasBinder;
class ClientContext;
class Expression;
class LogicalOperator;
class ParsedExpression;

//! The LATERAL binder is responsible for binding an expression within a LATERAL join
class LateralBinder : public ExpressionBinder {
public:
	LateralBinder(Binder &binder, ClientContext &context);

	bool HasCorrelatedColumns() const {
		return !correlated_columns.empty();
	}

	static void ReduceExpressionDepth(LogicalOperator &op, const CorrelatedColumns &info);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);
	void ExtractCorrelatedColumns(Expression &expr);

private:
	CorrelatedColumns correlated_columns;
};

} // namespace duckdb
