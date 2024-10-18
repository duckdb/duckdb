//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/rewrite_subquery.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//! Helper class to rewrite correlated cte scans within a single LogicalOperator
class RewriteSubquery : public LogicalOperatorVisitor {
public:
	RewriteSubquery(const idx_t table_index, idx_t lateral_depth,
	                const vector<CorrelatedColumnInfo> &correlated_columns);

	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	const idx_t table_index;
	idx_t lateral_depth;
	vector<CorrelatedColumnInfo> correlated_columns;

	vector<CorrelatedColumnInfo> add_correlated_columns;
};

class RewriteCorrelatedSubqueriesRecursive : public BoundNodeVisitor {
public:
	RewriteCorrelatedSubqueriesRecursive(const idx_t table_index, idx_t lateral_depth,
	                                     const vector<CorrelatedColumnInfo> &correlated_columns);

	void VisitBoundTableRef(BoundTableRef &ref) override;
	void VisitExpression(unique_ptr<Expression> &expression) override;

	void RewriteCorrelatedSubquery(Binder &binder, BoundQueryNode &subquery);

	const idx_t table_index;
	idx_t lateral_depth;
	idx_t subquery_depth = 0;
	const vector<CorrelatedColumnInfo> &correlated_columns;
	unique_ptr<Expression> condition;
	Binder *binder;

	vector<CorrelatedColumnInfo> add_correlated_columns;
};

} // namespace duckdb
