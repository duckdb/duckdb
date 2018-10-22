//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_cross_product.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalOperator {
  public:
	LogicalCrossProduct()
	    : LogicalOperator(LogicalOperatorType::CROSS_PRODUCT) {
	}

	virtual void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
};
} // namespace duckdb
