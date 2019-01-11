//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/cse_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"
#include "planner/logical_operator_visitor.hpp"

namespace duckdb {
//! The CommonSubExpression optimizer traverses the expressions of a LogicalOperator to look for duplicate expressions, and moves duplicate expressions into a shared CommonSubExpression.
class CommonSubExpressionOptimizer : public LogicalOperatorVisitor {
private:
	struct CSENode {
		size_t count;
		Expression *expr;

		CSENode(size_t count = 1, Expression *expr = nullptr) : count(count), expr(expr) {
		}
	};
	typedef unordered_map<Expression *, CSENode, ExpressionHashFunction, ExpressionEquality> expression_map_t;

	//! First iteration: count how many times each expression occurs
	void CountExpressions(Expression *expr, expression_map_t &expression_count);
	//! Second iteration: perform the actual replacement of the duplicate expressions with common subexpressions nodes
	Expression *PerformCSEReplacement(Expression *expr, expression_map_t &expression_count);

	//! Main method to extract common subexpressions
	void ExtractCommonSubExpresions(LogicalOperator &op);
public:
	using LogicalOperatorVisitor::Visit;
	void Visit(LogicalFilter &op) override;
	void Visit(LogicalProjection &op) override;
};
}
