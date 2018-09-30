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
	std::unique_ptr<Expression> left;
	std::unique_ptr<Expression> right;
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
	void SetJoinCondition(std::unique_ptr<Expression> condition);

	std::vector<JoinCondition> conditions;
	JoinType type;
	static JoinSide GetJoinSide(LogicalOperator *op,
	                            std::unique_ptr<Expression> &expr);

	virtual std::string ParamsToString() const override {
		std::string result = "";
		if (conditions.size() > 0) {
			result += "[";
			for (size_t i = 0; i < conditions.size(); i++) {
				auto &cond = conditions[i];
				result += ExpressionTypeToString(cond.comparison) + "(" +
				          cond.left->ToString() + ", " +
				          cond.right->ToString() + ")";
				if (i < conditions.size() - 1) {
					result += ", ";
				}
			}
			result += "]";
		}

		return result;
	}

  private:
};
} // namespace duckdb
