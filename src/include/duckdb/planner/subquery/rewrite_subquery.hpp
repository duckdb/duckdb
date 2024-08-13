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

namespace duckdb {

//! Helper class to rewrite correlated cte scans within a single LogicalOperator
class RewriteSubquery : public LogicalOperatorVisitor {
public:
	RewriteSubquery(idx_t table_index, idx_t lateral_depth, ColumnBinding base_binding,
	                const vector<CorrelatedColumnInfo> &correlated_columns,
	                column_binding_map_t<idx_t> &correlated_map);

	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	idx_t table_index;
	idx_t lateral_depth;
	ColumnBinding base_binding;
	const vector<CorrelatedColumnInfo> &correlated_columns;
	column_binding_map_t<idx_t> &correlated_map;
};

class RewriteCorrelatedSubqueriesRecursive : public BoundNodeVisitor {
public:
	RewriteCorrelatedSubqueriesRecursive(idx_t table_index, idx_t lateral_depth, ColumnBinding base_binding,
	                                     const vector<CorrelatedColumnInfo> &correlated_columns,
	                                     column_binding_map_t<idx_t> &correlated_map);

	void VisitBoundTableRef(BoundTableRef &ref) override;
	void VisitExpression(unique_ptr<Expression> &expression) override;

	void RewriteCorrelatedSubquery(Binder &binder, BoundQueryNode &subquery, bool add_filter = false);

	idx_t table_index;
	idx_t lateral_depth;
	ColumnBinding base_binding;
	const vector<CorrelatedColumnInfo> &correlated_columns;
	column_binding_map_t<idx_t> &correlated_map;
	bool add_filter = false;
	unique_ptr<Expression> condition;
	vector<CorrelatedColumnInfo> add_correlation;
};

} // namespace duckdb
