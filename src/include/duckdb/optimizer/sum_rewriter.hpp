//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/sum_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class ExpressionMatcher;
class Optimizer;

//! Rewrites SUM(x + C) into SUM(x) + C * COUNT(x)
class SumRewriterOptimizer : public LogicalOperatorVisitor {
public:
	explicit SumRewriterOptimizer(Optimizer &optimizer);
	~SumRewriterOptimizer() override;

	void Optimize(unique_ptr<LogicalOperator> &op);
	void VisitOperator(LogicalOperator &op) override;

private:
	void StandardVisitOperator(LogicalOperator &op);
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	void RewriteSums(unique_ptr<LogicalOperator> &aggr);

private:
	Optimizer &optimizer;
	column_binding_map_t<ColumnBinding> aggregate_map;
	unique_ptr<ExpressionMatcher> sum_matcher;
};
} // namespace duckdb
