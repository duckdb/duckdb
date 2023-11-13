//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/rewrite_correlated_expressions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Helper class to rewrite correlated expressions within a single LogicalOperator
class RewriteCorrelatedExpressions : public LogicalOperatorVisitor {
public:
	RewriteCorrelatedExpressions(ColumnBinding base_binding, column_binding_map_t<idx_t> &correlated_map,
	                             idx_t lateral_depth, bool recursive_rewrite = false);

	void VisitOperator(LogicalOperator &op) override;

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	//! Helper class used to recursively rewrite correlated expressions within nested subqueries.
	class RewriteCorrelatedRecursive {
	public:
		RewriteCorrelatedRecursive(BoundSubqueryExpression &parent, ColumnBinding base_binding,
		                           column_binding_map_t<idx_t> &correlated_map);
		void RewriteJoinRefRecursive(BoundTableRef &ref);
		void RewriteCorrelatedSubquery(BoundSubqueryExpression &expr);
		void RewriteCorrelatedExpressions(Expression &child);

		BoundSubqueryExpression &parent;
		ColumnBinding base_binding;
		column_binding_map_t<idx_t> &correlated_map;
	};

private:
	ColumnBinding base_binding;
	column_binding_map_t<idx_t> &correlated_map;
	// To keep track of the number of dependent joins encountered
	idx_t lateral_depth;
	// This flag is used to determine if the rewrite should recursively update the bindings for all
	// bound columns ref in the plan, and update the depths to match the new source
	bool recursive_rewrite;
};

//! Helper class that rewrites COUNT aggregates into a CASE expression turning NULL into 0 after a LEFT OUTER JOIN
class RewriteCountAggregates : public LogicalOperatorVisitor {
public:
	explicit RewriteCountAggregates(column_binding_map_t<idx_t> &replacement_map);

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	column_binding_map_t<idx_t> &replacement_map;
};

} // namespace duckdb
