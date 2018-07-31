
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalOrder : public LogicalOperator {
  public:
	LogicalOrder(OrderByDescription description)
	    : LogicalOperator(LogicalOperatorType::ORDER_BY),
	      description(std::move(description)) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	OrderByDescription description;
};
} // namespace duckdb