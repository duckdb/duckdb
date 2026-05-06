//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/partitioned_execution.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;

class PartitionedExecution {
public:
	PartitionedExecution(Optimizer &optimizer, unique_ptr<LogicalOperator> &root);

public:
	void Optimize(unique_ptr<LogicalOperator> &op);

private:
	Optimizer &optimizer;
	unique_ptr<LogicalOperator> &root;
	const idx_t num_threads;
};

} // namespace duckdb
