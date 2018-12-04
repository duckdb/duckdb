//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_aggregate.hpp
// 
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
	LogicalAggregate(std::vector<std::unique_ptr<Expression>> select_list)
	    : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY,
	                      std::move(select_list)) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	std::vector<string> GetNames() override;
	
	//! The set of groups (optional).
	std::vector<std::unique_ptr<Expression>> groups;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void SetExpression(size_t index,
	                   std::unique_ptr<Expression> expr) override;

	std::string ParamsToString() const override;
  protected:
	void ResolveTypes() override;
};
} // namespace duckdb
