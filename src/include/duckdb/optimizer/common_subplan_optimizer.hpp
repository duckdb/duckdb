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

//! The CommonSubplanOptimizer optimizer converts duplicate subplans to a materialized CTE
class CommonSubplanOptimizer {
public:
	explicit CommonSubplanOptimizer(Optimizer &optimizer);

public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! The optimizer
	Optimizer &optimizer;
};

} // namespace duckdb
