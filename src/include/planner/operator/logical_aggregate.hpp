//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_aggregate.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator {
  public:
	LogicalAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list)
	    : LogicalOperator(LogicalOperatorType::AGGREGATE_AND_GROUP_BY),
	      select_list(move(select_list)) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! The selection list of group columns and aggregates
	std::vector<std::unique_ptr<AbstractExpression>> select_list;
	//! The set of groups (optional).
	std::vector<std::unique_ptr<AbstractExpression>> groups;
};
} // namespace duckdb
