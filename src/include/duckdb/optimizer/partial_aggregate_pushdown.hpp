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
	bool TryPushdownAggregate(unique_ptr<LogicalOperator> &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> replacement_map;
};
} // namespace duckdb
