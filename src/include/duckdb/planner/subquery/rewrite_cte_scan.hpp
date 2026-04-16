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
	//! Rewrite LOGICAL_CTE_REF nodes and non-recursive dependent joins that reference the target CTE.
	WITH_NON_RECURSIVE_DEPENDENT_JOINS,
	//! Rewrite LOGICAL_CTE_REF nodes and dependent joins for recursive CTE rewrites (preserving recursive order).
	WITH_RECURSIVE_DEPENDENT_JOINS
};

//! Helper class to rewrite correlated cte scans within a single LogicalOperator
class RewriteCTEScan : public LogicalOperatorVisitor {
public:
	RewriteCTEScan(TableIndex table_index, const CorrelatedColumns &correlated_columns,
	               const reference_set_t<LogicalOperator> &accessing_operators, CTEScanRewriteMode mode);

	void VisitOperator(LogicalOperator &op) override;

private:
	TableIndex table_index;
	const CorrelatedColumns &correlated_columns;
	CTEScanRewriteMode mode;
	const_reference<reference_set_t<LogicalOperator>> accessing_operators;
};

} // namespace duckdb
