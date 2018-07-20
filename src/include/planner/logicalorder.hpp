
#pragma once

#include "planner/logicaloperator.hpp"

namespace duckdb {

class LogicalOrder : public LogicalOperator {
 public:
 	LogicalOrder() : 
 		LogicalOperator(LogicalOperatorType::ORDER_BY) { }

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	OrderByDescription description;
};

}