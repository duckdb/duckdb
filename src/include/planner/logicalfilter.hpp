
#pragma once

#include "planner/logicaloperator.hpp"

namespace duckdb {

class LogicalFilter : public LogicalOperator {
 public:
	LogicalFilter(std::unique_ptr<AbstractExpression> expression);

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	std::vector<std::unique_ptr<AbstractExpression>> expressions;
 private:
 	void SplitPredicates(std::unique_ptr<AbstractExpression> expression);
};

}