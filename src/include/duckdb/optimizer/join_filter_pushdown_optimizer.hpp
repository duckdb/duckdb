//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_filter_pushdown_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {
class Optimizer;

//! The JoinFilterPushdownOptimizer links comparison joins to data sources to enable dynamic execution-time filter
//! pushdown
class JoinFilterPushdownOptimizer : public LogicalOperatorVisitor {
public:
	explicit JoinFilterPushdownOptimizer(Optimizer &optimizer);

	void VisitOperator(LogicalOperator &op) override;

private:
	void GenerateJoinFilters(LogicalComparisonJoin &join);

private:
	Optimizer &optimizer;
};
} // namespace duckdb
