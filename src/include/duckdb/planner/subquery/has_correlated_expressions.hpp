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
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class BoundColumnRefExpression;
class BoundSubqueryExpression;
class LogicalOperator;
struct CorrelatedColumns;

//! Helper class to recursively detect correlated expressions inside a single LogicalOperator
class HasCorrelatedExpressions : public LogicalOperatorVisitor {
public:
	explicit HasCorrelatedExpressions(const CorrelatedColumns &correlated, bool lateral = false,
	                                  idx_t lateral_depth = 0);

	void VisitOperator(LogicalOperator &op) override;

	bool has_correlated_expressions;
	bool lateral;

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	const CorrelatedColumns &correlated_columns;
	// Tracks number of nested laterals
	idx_t lateral_depth;
};

} // namespace duckdb
