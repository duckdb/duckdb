//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_except.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalExcept : public LogicalOperator {
public:
	LogicalExcept(unique_ptr<LogicalOperator> top_select, unique_ptr<LogicalOperator> bottom_select)
	    : LogicalOperator(LogicalOperatorType::EXCEPT) {
		AddChild(move(top_select));
		AddChild(move(bottom_select));
	}

	vector<string> GetNames() override {
		return children[0]->GetNames();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
