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

//! Detects common subplans and converts them to references of a shared CTE
class CommonSubplanOptimizer {
public:
	explicit CommonSubplanOptimizer(Optimizer &optimizer);

public:
	//! Before filter pushdown, only consider filtered inputs with common connecting join predicates
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op, bool before_filter_pushdown = false);

private:
	//! The optimizer
	Optimizer &optimizer;
};

} // namespace duckdb
