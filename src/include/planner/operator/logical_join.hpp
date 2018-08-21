//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

struct JoinCondition {
	std::unique_ptr<AbstractExpression> left;
	std::unique_ptr<AbstractExpression> right;
	ExpressionType comparison;
};

enum JoinSide { NONE, LEFT, RIGHT, BOTH };

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
  public:
	LogicalJoin(JoinType type)
	    : LogicalOperator(LogicalOperatorType::JOIN), type(type) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! Creates the join condition for this node from the given expression
	void SetJoinCondition(std::unique_ptr<AbstractExpression> condition);

	std::vector<JoinCondition> conditions;
	JoinType type;

  private:
	JoinSide GetJoinSide(std::unique_ptr<AbstractExpression> &expr);
};
} // namespace duckdb
