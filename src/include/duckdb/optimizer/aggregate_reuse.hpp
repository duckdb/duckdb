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

//! Reuses an exact aggregate payload already computed by a filtering SEMI join.
class AggregateReuseOptimizer : public LogicalOperatorVisitor {
public:
	void VisitOperator(unique_ptr<LogicalOperator> &op) override;

private:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expression,
	                                    unique_ptr<Expression> *expression_ptr) override;
	bool TryRewrite(unique_ptr<LogicalOperator> &op);

private:
	column_binding_map_t<ColumnBinding> replacement_map;
};

} // namespace duckdb
