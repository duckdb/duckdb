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
	static void Rewrite(LogicalOperator &op, vector<ColumnBinding> correlated_bindings,
	                    column_binding_map_t<idx_t> &correlated_map, bool recursive_rewrite = false,
	                    optional_ptr<column_binding_map_t<ColumnBinding>> equivalent_bindings = nullptr);

private:
	RewriteCorrelatedExpressions(vector<ColumnBinding> correlated_bindings, column_binding_map_t<idx_t> &correlated_map,
	                             bool recursive_rewrite = false,
	                             optional_ptr<column_binding_map_t<ColumnBinding>> equivalent_bindings = nullptr);
	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	vector<ColumnBinding> correlated_bindings;
	column_binding_map_t<idx_t> &correlated_map;
	optional_ptr<column_binding_map_t<ColumnBinding>> equivalent_bindings;
	// This flag is used to determine if the rewrite should recursively update the bindings for all
	// bound columns ref in the plan, and update the depths to match the new source
	bool recursive_rewrite;
};

//! Helper class that rewrites COUNT aggregates into a CASE expression turning NULL into 0 after a LEFT OUTER JOIN
class RewriteCountAggregates : public LogicalOperatorVisitor {
public:
	static void Rewrite(LogicalOperator &op, column_binding_map_t<idx_t> &replacement_map);

private:
	explicit RewriteCountAggregates(column_binding_map_t<idx_t> &replacement_map);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	column_binding_map_t<idx_t> &replacement_map;
};

} // namespace duckdb
