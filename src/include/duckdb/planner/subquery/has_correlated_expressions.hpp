//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/has_correlated_expressions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Helper class to recursively detect correlated expressions inside a single LogicalOperator
class HasCorrelatedExpressions : public LogicalOperatorVisitor {
public:
	static bool Detect(LogicalOperator &op, const CorrelatedColumns &correlated, bool lateral = false,
	                   idx_t lateral_depth = 0);

private:
	explicit HasCorrelatedExpressions(const CorrelatedColumns &correlated, bool lateral = false,
	                                  idx_t lateral_depth = 0);
	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	bool has_correlated_expressions;
	bool lateral;
	const CorrelatedColumns &correlated_columns;
	// Tracks number of nested laterals
	idx_t lateral_depth;
};

} // namespace duckdb
