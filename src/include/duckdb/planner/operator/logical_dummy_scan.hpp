//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/include/duckdb/planner/operator/logical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDummyScan represents a dummy scan returning a single row
class LogicalDummyScan : public LogicalOperator {
public:
	LogicalDummyScan(idx_t table_index) : LogicalOperator(LogicalOperatorType::DUMMY_SCAN), table_index(table_index) {}

	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(table_index, 0)};
	}

	idx_t EstimateCardinality() override {
		return 1;
	}
protected:
	void ResolveTypes() override {
		// already resolved
	}
};
} // namespace duckdb
