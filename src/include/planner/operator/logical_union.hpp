//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_union.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalUnion : public LogicalOperator {
  public:
	LogicalUnion(std::unique_ptr<LogicalOperator> top_select,
	             std::unique_ptr<LogicalOperator> bottom_select)
	    : LogicalOperator(LogicalOperatorType::UNION) {
		AddChild(move(top_select));
		AddChild(move(bottom_select));
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
};
} // namespace duckdb
