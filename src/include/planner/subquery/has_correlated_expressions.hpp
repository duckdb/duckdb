//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/subquery/has_correlated_expressions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/binder.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! Helper class to recursively detect correlated expressions inside a single LogicalOperator
class HasCorrelatedExpressions : public LogicalOperatorVisitor {
public:
	HasCorrelatedExpressions(const vector<CorrelatedColumnInfo> &correlated);

	void VisitOperator(LogicalOperator &op) override;

	void Visit(BoundColumnRefExpression &expr) override;
	void Visit(BoundSubqueryExpression &expr) override;

	bool has_correlated_expressions;
	const vector<CorrelatedColumnInfo> &correlated_columns;
};

} // namespace duckdb
