//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/lateral_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class ColumnAliasBinder;

//! The LATERAL binder is responsible for binding an expression within a LATERAL join
class LateralBinder : public ExpressionBinder {
public:
	LateralBinder(Binder &binder, ClientContext &context);

	bool HasCorrelatedColumns() const {
		return !correlated_columns.empty();
	}

	static void ReduceExpressionDepth(LogicalOperator &op, const vector<CorrelatedColumnInfo> &info);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
	                          bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	BindResult BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);
	void ExtractCorrelatedColumns(Expression &expr);

private:
	vector<CorrelatedColumnInfo> correlated_columns;
};

} // namespace duckdb
