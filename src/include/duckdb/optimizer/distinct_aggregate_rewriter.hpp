//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/distinct_aggregate_rewriter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Optimizer;

//! The DistinctAggregateRewriter turns each DISTINCT argument/filter set into a deduplication aggregate followed by
//! a non-distinct aggregate. Multiple sets share the original input through a CTE and join on the original group keys.
class DistinctAggregateRewriter : public LogicalOperatorVisitor {
public:
	explicit DistinctAggregateRewriter(Optimizer &optimizer);

	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	bool TryRewrite(unique_ptr<LogicalOperator> &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> replacement_map;
};

} // namespace duckdb
