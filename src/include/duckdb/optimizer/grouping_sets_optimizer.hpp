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

//! The GroupingSetsOptimizer rewrites aggregates over multiple grouping sets (ROLLUP/CUBE/GROUPING SETS) into
//! explicit aggregate branches. Compatible aggregates use a cascade that combines exported states; other aggregates
//! use one branch per grouping set over a shared materialized input.
class GroupingSetsOptimizer : public LogicalOperatorVisitor {
public:
	explicit GroupingSetsOptimizer(Optimizer &optimizer);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	bool TryRewriteGroupingSets(unique_ptr<LogicalOperator> &op);
	bool TryExpandGroupingSets(unique_ptr<LogicalOperator> &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> replacement_map;
};
} // namespace duckdb
