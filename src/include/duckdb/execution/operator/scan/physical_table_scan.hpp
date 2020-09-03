//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
public:
	PhysicalTableScan(vector<LogicalType> types,
	                  TableFunction function,
	                  unique_ptr<FunctionData> bind_data,
	                  vector<column_t> column_ids,
	                  unordered_map<idx_t, vector<TableFilter>> table_filters);

	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The projected-out column ids
	vector<column_t> column_ids;
	//! The table filters
	unordered_map<idx_t, vector<TableFilter>> table_filters;
public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	string ToString(idx_t depth = 0) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	void ParallelScanInfo(ClientContext &context, std::function<void(unique_ptr<OperatorTaskInfo>)> callback) override;
};

} // namespace duckdb
