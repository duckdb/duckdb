//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalAggregate;
class LogicalGet;

class AggregatePushdown {
public:
	explicit AggregatePushdown(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	//! Try to push down the aggregate at op into its LogicalGet child.
	//! Keeps LogicalAggregate but rewrites it to consume partial results from scan workers.
	//! Returns true and rewrites op in-place when pushdown succeeds.
	bool TryPushdown(unique_ptr<LogicalOperator> &op);

	//! Recursively walk the plan, calling TryPushdown at every LogicalAggregate node.
	void VisitOperator(unique_ptr<LogicalOperator> &op);

	Optimizer &optimizer;
	//! Root of the plan — needed by ColumnBindingReplacer::VisitOperator
	optional_ptr<LogicalOperator> root;
};

} // namespace duckdb
