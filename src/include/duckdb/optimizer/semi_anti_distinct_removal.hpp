//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/semi_anti_distinct_removal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

//! Removes a DISTINCT that sits directly on the existence-only side of a semi,
//! anti or mark join. Those joins only ask whether a match exists, so duplicates
//! on that side cannot change the result.
class SemiAntiDistinctRemoval {
public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether the DISTINCT below this join can be removed
	static bool CanOptimize(LogicalOperator &op);
};

} // namespace duckdb
