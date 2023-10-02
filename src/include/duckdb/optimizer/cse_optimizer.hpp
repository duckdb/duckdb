//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cse_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Binder;
struct CSEReplacementState;

//! The CommonSubExpression optimizer traverses the expressions of a LogicalOperator to look for duplicate expressions
//! if there are any, it pushes a projection under the operator that resolves these expressions
class CommonSubExpressionOptimizer : public LogicalOperatorVisitor {
public:
	explicit CommonSubExpressionOptimizer(Binder &binder) : binder(binder) {
	}

public:
	void VisitOperator(LogicalOperator &op) override;

private:
	//! First iteration: count how many times each expression occurs
	void CountExpressions(Expression &expr, CSEReplacementState &state);
	//! Second iteration: perform the actual replacement of the duplicate expressions with common subexpressions nodes
	void PerformCSEReplacement(unique_ptr<Expression> &expr, CSEReplacementState &state);

	//! Main method to extract common subexpressions
	void ExtractCommonSubExpresions(LogicalOperator &op);

private:
	Binder &binder;
};
} // namespace duckdb
