//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
  public:
	LogicalJoin(JoinType type)
	    : LogicalOperator(LogicalOperatorType::JOIN), type(type) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	JoinType type;
};
} // namespace duckdb
