//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_reuse.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class LogicalMaterializedCTE;
class Optimizer;

//! Reuses grouped aggregate work already available through SEMI joins or materialized CTEs.
class AggregateReuseOptimizer : public LogicalOperatorVisitor {
public:
	explicit AggregateReuseOptimizer(Optimizer &optimizer);

	void CollectCTEs(LogicalOperator &op);
	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expression,
	                                    unique_ptr<Expression> *expression_ptr) override;
	bool TryReuseSemiAggregate(unique_ptr<LogicalOperator> &op);
	bool TryReuseMaterializedAggregate(unique_ptr<LogicalOperator> &op);

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> replacement_map;
	unordered_map<idx_t, reference<LogicalMaterializedCTE>> cte_definitions;
};

} // namespace duckdb
