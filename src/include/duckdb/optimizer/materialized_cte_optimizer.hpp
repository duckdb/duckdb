//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/materialized_cte_optimizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;

class MaterializedCTEOptimizer {
public:
	explicit MaterializedCTEOptimizer(Optimizer &optimizer);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	void VisitOperator(LogicalOperator &op);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
