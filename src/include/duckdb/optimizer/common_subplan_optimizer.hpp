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

//! The CommonAggregateOptimizer optimizer eliminates duplicate aggregates from aggregate nodes
class CommonSubplanOptimizer {
public:
	explicit CommonSubplanOptimizer(Optimizer &optimizer);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! The optimizer
	Optimizer &optimizer;
};

} // namespace duckdb
