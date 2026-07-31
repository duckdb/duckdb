//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/multi_stage_aggregate_rewriter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Optimizer;

//! Rewrites aggregates that require multiple aggregate stages. Compatible branches share their input through a CTE
//! and are joined on the original group keys.
class MultiStageAggregateRewriter : public LogicalOperatorVisitor {
public:
	MultiStageAggregateRewriter(Optimizer &optimizer, bool rewrite_distinct, bool rewrite_frequency);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	bool TryRewrite(unique_ptr<LogicalOperator> &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	bool rewrite_distinct;
	bool rewrite_frequency;
	column_binding_map_t<ColumnBinding> replacement_map;
};

} // namespace duckdb
