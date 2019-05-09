//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_any_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! LogicalAnyJoin represents a join with an arbitrary expression as JoinCondition
class LogicalAnyJoin : public LogicalJoin {
public:
	LogicalAnyJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::ANY_JOIN);

	//! The JoinCondition on which this join is performed
	unique_ptr<Expression> condition;

	uint64_t ExpressionCount() override;
	Expression *GetExpression(uint64_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       uint64_t index) override;

	string ParamsToString() const override;
};

} // namespace duckdb
