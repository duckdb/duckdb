
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalAggregate : public LogicalOperator {
 public:
 	LogicalAggregate(std::vector<std::unique_ptr<AbstractExpression>> select_list) : 
 		LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY), select_list(move(select_list)) { }

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
	std::vector<std::unique_ptr<AbstractExpression>> groups;
};


}