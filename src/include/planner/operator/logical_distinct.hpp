//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_distinct.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDistinct filters duplicate entries from its child operator
class LogicalDistinct : public LogicalOperator {
public:
	LogicalDistinct() : LogicalOperator(LogicalOperatorType::DISTINCT) {
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
