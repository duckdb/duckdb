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
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
public:
	PhysicalTableScan(vector<LogicalType> types, TableFunction function, unique_ptr<FunctionData> bind_data,
	                  vector<column_t> column_ids, vector<string> names, unique_ptr<TableFilterSet> table_filters,
	                  idx_t estimated_cardinality);

	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The projected-out column ids
	vector<column_t> column_ids;
	//! The names of the columns
	vector<string> names;
	//! The table filters
	unique_ptr<TableFilterSet> table_filters;

public:
	string GetName() const override;
	string ParamsToString() const override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
