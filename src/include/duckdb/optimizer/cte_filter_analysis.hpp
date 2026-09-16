//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cte_filter_analysis.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class LogicalMaterializedCTE;

//! Proves that filtered row consumers also supply the keys needed by other consumers.
//! Analysis borrows an immutable plan and must not survive a rewrite of that plan.
class CTEFilterAnalysis {
public:
	static bool CanRestrict(LogicalOperator &root, LogicalMaterializedCTE &cte,
	                        const vector<reference<LogicalOperator>> &filters);
};
} // namespace duckdb
