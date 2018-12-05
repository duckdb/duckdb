//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

struct JoinCondition {
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
};

enum JoinSide { NONE, LEFT, RIGHT, BOTH };

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	LogicalJoin(JoinType type) : LogicalOperator(LogicalOperatorType::JOIN), type(type) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
	vector<string> GetNames() override;

	//! Creates the join condition for this node from the given expression
	void SetJoinCondition(unique_ptr<Expression> condition);

	vector<JoinCondition> conditions;
	JoinType type;
	static JoinSide GetJoinSide(LogicalOperator *op, unique_ptr<Expression> &expr);

	string ParamsToString() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
