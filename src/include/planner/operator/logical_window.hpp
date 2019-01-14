//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalWindow : public LogicalOperator {
public:
	LogicalWindow() : LogicalOperator(LogicalOperatorType::WINDOW) {
	}

	vector<string> GetNames() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
