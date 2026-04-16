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

//! Helper class to rewrite correlated CTE scans and dependent joins within a single LogicalOperator
class RewriteCTEScan : public LogicalOperatorVisitor {
public:
	static void Rewrite(LogicalOperator &op, TableIndex table_index, const CorrelatedColumns &correlated_columns);

private:
	RewriteCTEScan(TableIndex table_index, const CorrelatedColumns &correlated_columns);
	bool CollectAccessingOperators(LogicalOperator &op);
	void VisitOperator(LogicalOperator &op) override;

	TableIndex table_index;
	const CorrelatedColumns &correlated_columns;
	reference_set_t<LogicalOperator> accessing_operators;
};

} // namespace duckdb
