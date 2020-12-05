//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalSample represents a SAMPLE clause
class LogicalSample : public LogicalOperator {
public:
	LogicalSample(int64_t sample_size_p, unique_ptr<LogicalOperator> child)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE), sample_size(sample_size_p) {
		children.push_back(move(child));
	}

	//! The number of elements to sample
	int64_t sample_size;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

} // namespace duckdb
