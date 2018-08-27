#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalExplain : public LogicalOperator {
  public:
	LogicalExplain(std::unique_ptr<LogicalOperator> plan)
	    : LogicalOperator(LogicalOperatorType::EXPLAIN), logical_plan(move(plan)) {}
	std::unique_ptr<LogicalOperator> logical_plan;
	std::string physical_plan;
	std::string parse_tree;
	std::string logical_plan_unopt;

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

};
} // namespace duckdb
