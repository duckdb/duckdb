//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/scalar_aggregate_fusion.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class Optimizer;

//! Fuses cross products of scalar aggregate branches over the same input.
//!
//! This targets plans like:
//!   FROM (SELECT count(*) FROM R WHERE C AND P1),
//!        (SELECT count(*) FROM R WHERE C AND P2), ...
//!
//! and rewrites them into one scan of R with filtered aggregates.
class ScalarAggregateFusion {
public:
	explicit ScalarAggregateFusion(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
