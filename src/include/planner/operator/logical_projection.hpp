
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalProjection : public LogicalOperator {
  public:
	LogicalProjection(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : LogicalOperator(LogicalOperatorType::PROJECTION),
	      select_list(move(select_list)) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
};
} // namespace duckdb