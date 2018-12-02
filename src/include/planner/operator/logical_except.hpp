//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_except.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalExcept : public LogicalOperator {
  public:
	LogicalExcept(std::unique_ptr<LogicalOperator> top_select,
	              std::unique_ptr<LogicalOperator> bottom_select)
	    : LogicalOperator(LogicalOperatorType::EXCEPT) {
		AddChild(move(top_select));
		AddChild(move(bottom_select));
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
};
} // namespace duckdb
