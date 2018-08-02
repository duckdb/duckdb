//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_distinct.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDistinct represents a DISTINCT clause that filters duplicates from
//! the result.
class LogicalDistinct : public LogicalOperator {
  public:
	LogicalDistinct() : LogicalOperator(LogicalOperatorType::DISTINCT) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb