
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalLimit : public LogicalOperator {
  public:
	LogicalLimit(int64_t limit, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::LIMIT), limit(limit),
	      offset(offset) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	int64_t limit;
	int64_t offset;
};
}