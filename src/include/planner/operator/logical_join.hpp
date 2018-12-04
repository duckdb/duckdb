//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_join.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

struct JoinCondition {
	std::unique_ptr<Expression> left;
	std::unique_ptr<Expression> right;
	ExpressionType comparison;
};

enum JoinSide { NONE, LEFT, RIGHT, BOTH };

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
  public:
	LogicalJoin(JoinType type)
	    : LogicalOperator(LogicalOperatorType::JOIN), type(type) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
	std::vector<string> GetNames() override;

	//! Creates the join condition for this node from the given expression
	void SetJoinCondition(std::unique_ptr<Expression> condition);

	std::vector<JoinCondition> conditions;
	JoinType type;
	static JoinSide GetJoinSide(LogicalOperator *op,
	                            std::unique_ptr<Expression> &expr);

	static ExpressionType NegateComparisionExpression(ExpressionType type);
	static ExpressionType FlipComparisionExpression(ExpressionType type);

	std::string ParamsToString() const override;
  protected:
	void ResolveTypes() override;
};
} // namespace duckdb
