//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_filter.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
  public:
	LogicalFilter(std::unique_ptr<AbstractExpression> expression);
	LogicalFilter();

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

  private:
	void SplitPredicates(std::unique_ptr<AbstractExpression> expression);
};

} // namespace duckdb
