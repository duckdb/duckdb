//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_positional_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalPositionalScan : public PhysicalOperator {
public:
	//! Regular Table Scan
	PhysicalPositionalScan(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
	                       unique_ptr<PhysicalOperator> right);

	//! The child table functions
	vector<unique_ptr<PhysicalOperator>> child_tables;

public:
	bool Equals(const PhysicalOperator &other) const override;

public:
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	double GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
};

} // namespace duckdb
