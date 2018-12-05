//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_intersect.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalIntersect : public LogicalOperator {
public:
	LogicalIntersect(unique_ptr<LogicalOperator> top_select, unique_ptr<LogicalOperator> bottom_select)
	    : LogicalOperator(LogicalOperatorType::INTERSECT) {
		AddChild(move(top_select));
		AddChild(move(bottom_select));
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
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
