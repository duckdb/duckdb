//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "common/unordered_set.hpp"
#include "planner/operator/logical_join.hpp"
#include "planner/joinside.hpp"

namespace duckdb {

//! LogicalComparisonJoin represents a join that involves comparisons between the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
public:
	LogicalComparisonJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::COMPARISON_JOIN);

	//! The conditions of the join
	vector<JoinCondition> conditions;

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
