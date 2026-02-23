//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/limited_distinct_aggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class LogicalOperator;
class LimitedDistinctAggregation {
public:
	//! Push a LIMIT hint into a DISTINCT operator as its soft limit for early termination.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op);
};

} // namespace duckdb
