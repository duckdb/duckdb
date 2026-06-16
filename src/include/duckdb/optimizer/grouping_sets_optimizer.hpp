//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/grouping_sets_optimizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class Optimizer;

//! The GroupingSetsOptimizer rewrites aggregates over multiple grouping sets (ROLLUP/CUBE/GROUPING SETS) into a
//! cascade of aggregations connected through materialized CTEs. The finest grouping set is computed over the base
//! data with the aggregate states exported, after which coarser grouping sets are computed by combining the states
//! of a finer grouping set - instead of re-scanning and re-aggregating the base data for every grouping set.
class GroupingSetsOptimizer : public LogicalOperatorVisitor {
public:
	explicit GroupingSetsOptimizer(Optimizer &optimizer);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	bool TryRewriteGroupingSets(unique_ptr<LogicalOperator> &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> replacement_map;
};
} // namespace duckdb
