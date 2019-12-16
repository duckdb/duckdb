//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_explain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalExplain : public LogicalOperator {
public:
	LogicalExplain(unique_ptr<LogicalOperator> plan) : LogicalOperator(LogicalOperatorType::EXPLAIN) {
		children.push_back(move(plan));
	}

	string physical_plan;
	string logical_plan_unopt;
	string logical_plan_opt;

protected:
	void ResolveTypes() override {
		types = {TypeId::VARCHAR, TypeId::VARCHAR};
	}
};
} // namespace duckdb
