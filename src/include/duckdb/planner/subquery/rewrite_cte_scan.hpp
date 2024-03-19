//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/rewrite_cte_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Helper class to rewrite correlated cte scans within a single LogicalOperator
class RewriteCTEScan : public LogicalOperatorVisitor {
public:
	RewriteCTEScan(idx_t table_index, const vector<CorrelatedColumnInfo> &correlated_columns);

	void VisitOperator(LogicalOperator &op) override;

private:
	idx_t table_index;
	const vector<CorrelatedColumnInfo> &correlated_columns;
};

} // namespace duckdb
