
#pragma once

#include "planner/logicaloperator.hpp"

namespace duckdb {

class LogicalAggregate : public LogicalOperator {
 public:
 	LogicalAggregate() : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY) { }

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	std::vector<std::unique_ptr<AbstractExpression>> groups;
};


}