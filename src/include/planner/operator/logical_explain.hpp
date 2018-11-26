#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalExplain : public LogicalOperator {
  public:
	LogicalExplain(std::unique_ptr<LogicalOperator> plan)
	    : LogicalOperator(LogicalOperatorType::EXPLAIN) {
		children.push_back(move(plan));
	}

	std::string physical_plan;
	std::string parse_tree;
	std::string logical_plan_unopt;
	std::string logical_plan_opt;

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
};
} // namespace duckdb
