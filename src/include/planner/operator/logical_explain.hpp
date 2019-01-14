//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_explain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalExplain : public LogicalOperator {
public:
	LogicalExplain(unique_ptr<LogicalOperator> plan) : LogicalOperator(LogicalOperatorType::EXPLAIN) {
		children.push_back(move(plan));
	}

	string physical_plan;
	string parse_tree;
	string logical_plan_unopt;
	string logical_plan_opt;

	vector<string> GetNames() override {
		return {"explain_key", "explain_value"};
	}

protected:
	void ResolveTypes() override {
		types = {TypeId::VARCHAR, TypeId::VARCHAR};
	}
};
} // namespace duckdb
