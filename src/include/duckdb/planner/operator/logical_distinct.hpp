//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_distinct.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDistinct filters duplicate entries from its child operator
class LogicalDistinct : public LogicalOperator {
public:
	LogicalDistinct() : LogicalOperator(LogicalOperatorType::DISTINCT) {
	}
	LogicalDistinct(vector<unique_ptr<Expression>> targets)
	    : LogicalOperator(LogicalOperatorType::DISTINCT), distinct_targets(move(targets)) {
	}
	//! The set of distinct targets (optional).
	vector<unique_ptr<Expression>> distinct_targets;

public:
	string ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
