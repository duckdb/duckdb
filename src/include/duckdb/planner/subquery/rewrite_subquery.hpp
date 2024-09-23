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
	RewriteSubquery(const vector<idx_t> &table_index, idx_t lateral_depth, ColumnBinding base_binding,
	                const vector<CorrelatedColumnInfo> &correlated_columns,
	                column_binding_map_t<idx_t> &correlated_map);

	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	const vector<idx_t> &table_index;
	idx_t lateral_depth;
	ColumnBinding base_binding;
	vector<CorrelatedColumnInfo> correlated_columns;
	column_binding_map_t<idx_t> &correlated_map;

	vector<CorrelatedColumnInfo> add_correlated_columns;
};

class RewriteCorrelatedSubqueriesRecursive : public BoundNodeVisitor {
public:
	RewriteCorrelatedSubqueriesRecursive(const vector<idx_t> &table_index, idx_t lateral_depth,
	                                     ColumnBinding base_binding,
	                                     const vector<CorrelatedColumnInfo> &correlated_columns,
	                                     column_binding_map_t<idx_t> &correlated_map);

	void VisitBoundTableRef(BoundTableRef &ref) override;
	void VisitExpression(unique_ptr<Expression> &expression) override;

	void RewriteCorrelatedSubquery(Binder &binder, BoundQueryNode &subquery);

	const vector<idx_t> &table_index;
	idx_t lateral_depth;
	idx_t subquery_depth = 0;
	ColumnBinding base_binding;
	const vector<CorrelatedColumnInfo> &correlated_columns;
	unique_ptr<Expression> condition;
	column_binding_map_t<idx_t> &correlated_map;
	Binder *binder;

	vector<CorrelatedColumnInfo> add_correlated_columns;
};

} // namespace duckdb
