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
	idx_t Cost(Expression &expr);

	unique_ptr<Expression> VisitReplace(BoundConjunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	//! Override this function to search for filter operators
	void VisitOperator(LogicalOperator &op) override;

private:
	unordered_map<std::string, idx_t> function_costs = {
	    {"+", 5},       {"-", 5},    {"&", 5},          {"#", 5},
	    {">>", 5},      {"<<", 5},   {"abs", 5},        {"*", 10},
	    {"%", 10},      {"/", 15},   {"date_part", 20}, {"year", 20},
	    {"round", 100}, {"~~", 200}, {"!~~", 200},      {"regexp_matches", 200},
	    {"||", 200}};

	idx_t ExpressionCost(BoundBetweenExpression &expr);
	idx_t ExpressionCost(BoundCaseExpression &expr);
	idx_t ExpressionCost(BoundCastExpression &expr);
	idx_t ExpressionCost(BoundComparisonExpression &expr);
	idx_t ExpressionCost(BoundConjunctionExpression &expr);
	idx_t ExpressionCost(BoundFunctionExpression &expr);
	idx_t ExpressionCost(BoundOperatorExpression &expr, ExpressionType expr_type);
	idx_t ExpressionCost(PhysicalType return_type, idx_t multiplier);
};
} // namespace duckdb
