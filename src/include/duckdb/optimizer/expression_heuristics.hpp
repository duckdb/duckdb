//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/expression_heuristics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class TableFilterSet;
class TableFilter;

class ExpressionHeuristics : public LogicalOperatorVisitor {
public:
	explicit ExpressionHeuristics(Optimizer &optimizer) : optimizer(optimizer) {
	}

	Optimizer &optimizer;
	unique_ptr<LogicalOperator> root;

public:
	//! Search for filters to be reordered
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
	//! Reorder the expressions of a filter
	void ReorderExpressions(vector<unique_ptr<Expression>> &expressions);
	//! Return the cost of an expression
	static idx_t Cost(const Expression &expr);

	static vector<idx_t> GetInitialOrder(const TableFilterSet &table_filters);

	unique_ptr<Expression> VisitReplace(BoundConjunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	//! Override this function to search for filter operators
	void VisitOperator(LogicalOperator &op) override;

private:
	static idx_t ExpressionCost(const BoundBetweenExpression &expr);
	static idx_t ExpressionCost(const BoundCaseExpression &expr);
	static idx_t ExpressionCost(const BoundCastExpression &expr);
	static idx_t ExpressionCost(const BoundComparisonExpression &expr);
	static idx_t ExpressionCost(const BoundConjunctionExpression &expr);
	static idx_t ExpressionCost(const BoundFunctionExpression &expr);
	static idx_t ExpressionCost(const BoundOperatorExpression &expr, ExpressionType expr_type);
	static idx_t ExpressionCost(PhysicalType return_type, idx_t multiplier);
	static idx_t Cost(const TableFilter &filter);
};
} // namespace duckdb
