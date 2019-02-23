//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

#include <unordered_set>

namespace duckdb {

struct JoinCondition {
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
	//! NULL values are equal for just THIS JoinCondition (instead of the entire join), only support by HashJoin and can
	//! only be used in equality comparisons
	bool null_values_are_equal = false;

	JoinCondition() : null_values_are_equal(false) {
	}
};

enum JoinSide { NONE, LEFT, RIGHT, BOTH };

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	LogicalJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::JOIN);

	vector<string> GetNames() override;

	// Gets the set of table references that are reachable from this node
	static void GetTableReferences(LogicalOperator &op, std::unordered_set<size_t> &bindings);

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! The type of the join (INNER, OUTER, etc...)
	JoinType type;
	
	string ParamsToString() const override;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
