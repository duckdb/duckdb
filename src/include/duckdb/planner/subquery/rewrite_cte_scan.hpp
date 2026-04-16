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
	RewriteCTEScan(TableIndex table_index, const CorrelatedColumns &correlated_columns,
	               const reference_set_t<LogicalOperator> &accessing_operators);

	void VisitOperator(LogicalOperator &op) override;

private:
	TableIndex table_index;
	const CorrelatedColumns &correlated_columns;
	const_reference<reference_set_t<LogicalOperator>> accessing_operators;
};

} // namespace duckdb
