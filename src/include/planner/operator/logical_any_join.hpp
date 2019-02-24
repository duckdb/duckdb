//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_any_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/operator/logical_join.hpp"
#include "parser/expression.hpp"

namespace duckdb {

//! LogicalAnyJoin represents a join with an arbitrary expression as JoinCondition
class LogicalAnyJoin : public LogicalJoin {
public:
	LogicalAnyJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::ANY_JOIN);

	//! The JoinCondition on which this join is performed
	unique_ptr<Expression> condition;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

	string ParamsToString() const override;
};

} // namespace duckdb
