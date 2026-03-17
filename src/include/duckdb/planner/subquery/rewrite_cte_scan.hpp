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

//! Controls whether RewriteCTEScan rewrites only CTE_REF nodes or also dependent joins.
enum class CTEScanRewriteMode {
	//! Only rewrite LOGICAL_CTE_REF nodes; do not rewrite LOGICAL_DEPENDENT_JOIN nodes.
	CTE_REF_ONLY,
	//! Rewrite LOGICAL_CTE_REF nodes and non-recursive dependent joins that reference the target CTE.
	WITH_NON_RECURSIVE_DEPENDENT_JOINS,
	//! Rewrite LOGICAL_CTE_REF nodes and dependent joins for recursive CTE rewrites (preserving recursive order).
	WITH_RECURSIVE_DEPENDENT_JOINS
};

//! Helper class to rewrite correlated cte scans within a single LogicalOperator
class RewriteCTEScan : public LogicalOperatorVisitor {
public:
	RewriteCTEScan(idx_t table_index, const CorrelatedColumns &correlated_columns,
	               CTEScanRewriteMode mode = CTEScanRewriteMode::CTE_REF_ONLY);

	void VisitOperator(LogicalOperator &op) override;

private:
	idx_t table_index;
	const CorrelatedColumns &correlated_columns;
	CTEScanRewriteMode mode;
};

} // namespace duckdb
