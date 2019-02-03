//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalPrepare : public LogicalOperator {
public:
	LogicalPrepare(string name, unique_ptr<LogicalOperator> logical_plan)
	    : LogicalOperator(LogicalOperatorType::PREPARE), name(name) {
		children.push_back(move(logical_plan));
	}

	string name;

	vector<string> GetNames() override {
		return {"Success"};
	}

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BOOLEAN);
	}
};
} // namespace duckdb
