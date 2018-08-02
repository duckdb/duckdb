//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_table_scan.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
  public:
	PhysicalTableScan(DataTable *table, std::vector<size_t> column_ids)
	    : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN), table(table),
	      column_ids(column_ids) {}

	DataTable *table;
	std::vector<size_t> column_ids;

	virtual void InitializeChunk(DataChunk &chunk) override;
	virtual void GetChunk(DataChunk &chunk,
	                      PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalTableScanOperatorState : public PhysicalOperatorState {
  public:
	PhysicalTableScanOperatorState(size_t current_offset)
	    : PhysicalOperatorState(nullptr), current_offset(current_offset) {}

	//! The current position in the scan
	size_t current_offset;
};
} // namespace duckdb
