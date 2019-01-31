//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator {
public:
	LogicalAggregate(size_t group_index, size_t aggregate_index, vector<unique_ptr<Expression>> select_list)
	    : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY, std::move(select_list)), group_index(group_index), aggregate_index(aggregate_index) {
	}

	vector<string> GetNames() override;

	//! The table index for the groups of the LogicalAggregate
	size_t group_index;
	//! The table index for the aggregates of the LogicalAggregate
	size_t aggregate_index;
	//! The set of groups (optional).
	vector<unique_ptr<Expression>> groups;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

	string ParamsToString() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
