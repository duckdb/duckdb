//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/partial_aggregate_pushdown.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
class Optimizer;

//! The PartialAggregatePushdown optimizer pushes SUM aggregates below joins when this can reduce join work
class PartialAggregatePushdown : public LogicalOperatorVisitor {
public:
	explicit PartialAggregatePushdown(Optimizer &optimizer);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	//! Inline any projection chain sitting between the aggregate and a comparison join into the aggregate's
	//! own expressions (semantics-preserving), so the join becomes the aggregate's direct child and the
	//! pushdown paths below can match it. Returns true if anything was fused.
	bool FuseInterveningProjections(LogicalOperator &op);
	//! Double-eager (Yan & Larson "eager group-by-count"): pre-aggregate BOTH join inputs by the join key and
	//! reconstruct the aggregates above by scaling each side's partial with the other side's row count.
	bool TryDoubleEagerPushdown(unique_ptr<LogicalOperator> &op);
	//! One-sided eager group-by: push a partial aggregate (via state export) below the join on one side only.
	bool TryPushdownAggregate(unique_ptr<LogicalOperator> &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> replacement_map;
};
} // namespace duckdb
