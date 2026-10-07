//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/multi_stage_aggregate_rewriter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Optimizer;
class BaseStatistics;
class BoundAggregateExpression;
class LogicalAggregate;

//! Rewrites aggregates that require multiple aggregate stages. Compatible branches share their input through a CTE
//! and are joined on the original group keys.
class MultiStageAggregateRewriter : public LogicalOperatorVisitor {
public:
	MultiStageAggregateRewriter(
	    Optimizer &optimizer, AggregateRewritePolicy policy, bool rewrite_distinct = false,
	    optional_ptr<const column_binding_map_t<unique_ptr<BaseStatistics>>> statistics = nullptr);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;
	bool WasChanged() const;

private:
	bool TryRewrite(unique_ptr<LogicalOperator> &op);
	bool ShouldRewrite(const BoundAggregateExpression &aggregate, const LogicalAggregate &op) const;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	AggregateRewritePolicy policy;
	bool rewrite_distinct;
	optional_ptr<const column_binding_map_t<unique_ptr<BaseStatistics>>> statistics;
	bool changed = false;
	column_binding_map_t<ColumnBinding> replacement_map;
};

} // namespace duckdb
