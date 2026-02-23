//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/limited_distinct_aggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

//! An optimizer rule that pushes a LIMIT hint into grouped aggregations
//! which don't require all rows in the group to be processed for correctness.
//! Example queries fitting this description are:
//! - SELECT DISTINCT l_orderkey FROM lineitem LIMIT 10;
//! - SELECT l_orderkey FROM lineitem GROUP BY l_orderkey LIMIT 10;
class LimitedDistinctAggregation {
public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! Propagate LIMIT value into DISTINCT for early termination
	static void PushdownLimitIntoDistinct(LogicalOperator &op);
};

} // namespace duckdb
