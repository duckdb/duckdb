//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/partial_aggregate_pushdown.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Optimizer;

//! The PartialAggregatePushdown optimizer pushes SUM aggregates below joins when this can reduce join work
class PartialAggregatePushdown : public LogicalOperatorVisitor {
public:
	explicit PartialAggregatePushdown(Optimizer &optimizer);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	bool TryPushdownAggregate(unique_ptr<LogicalOperator> &op);

private:
	Optimizer &optimizer;
};
} // namespace duckdb
