//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "parser/expression.hpp"
#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! JoinCondition represents a left-right comparison join condition
struct JoinCondition {
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
	//! NULL values are equal for just THIS JoinCondition (instead of the entire join).
	//! This is only supported by the HashJoin and can only be used in equality comparisons.
	bool null_values_are_equal = false;

	JoinCondition() : null_values_are_equal(false) {
	}
};

enum class JoinSide : uint8_t { NONE, LEFT, RIGHT, BOTH };

//! LogicalComparisonJoin represents a join that involves comparisons between the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
public:
	LogicalComparisonJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::COMPARISON_JOIN);

	//! The conditions of the join
	vector<JoinCondition> conditions;

	static unique_ptr<Expression> CreateExpressionFromCondition(JoinCondition cond);
	static JoinSide CombineJoinSide(JoinSide left, JoinSide right);
	static JoinSide GetJoinSide(size_t table_binding, unordered_set<size_t> &left_bindings,
	                            unordered_set<size_t> &right_bindings);
	static JoinSide GetJoinSide(Expression &expression, unordered_set<size_t> &left_bindings,
	                            unordered_set<size_t> &right_bindings);
	static JoinSide GetJoinSide(unordered_set<size_t> bindings, unordered_set<size_t> &left_bindings,
	                            unordered_set<size_t> &right_bindings);
	static unique_ptr<LogicalOperator> CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              unordered_set<size_t> &left_bindings,
	                                              unordered_set<size_t> &right_bindings,
	                                              vector<unique_ptr<Expression>> &expressions);
	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

	string ParamsToString() const override;
};

} // namespace duckdb
