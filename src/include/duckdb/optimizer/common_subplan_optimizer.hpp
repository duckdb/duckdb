//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/common_subplan_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;
class LogicalOperator;

//! The CommonSubplanOptimizer optimizer detects common subplans, and converts them to refs of a materialized CTE
class CommonSubplanOptimizer {
public:
	explicit CommonSubplanOptimizer(Optimizer &optimizer);

public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! The optimizer
	Optimizer &optimizer;
};

//! The UnionAggregateMerger computes the branches of a UNION ALL that are the same aggregate over the same input,
//! restricted to a different value of a column each, with one aggregate that groups by the column
class UnionAggregateMerger {
public:
	explicit UnionAggregateMerger(Optimizer &optimizer);

public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! The optimizer
	Optimizer &optimizer;
};

} // namespace duckdb
