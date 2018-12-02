//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_intersect.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalIntersect : public LogicalOperator {
  public:
	LogicalIntersect(std::unique_ptr<LogicalOperator> top_select,
	                 std::unique_ptr<LogicalOperator> bottom_select)
	    : LogicalOperator(LogicalOperatorType::INTERSECT) {
		AddChild(move(top_select));
		AddChild(move(bottom_select));
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
};
} // namespace duckdb
