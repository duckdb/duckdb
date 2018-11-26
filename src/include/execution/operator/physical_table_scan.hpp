//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_table_scan.hpp
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
	PhysicalTableScan(TableCatalogEntry &tableref, DataTable &table,
	                  std::vector<column_t> column_ids)
	    : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN), tableref(tableref),
	      table(table), column_ids(column_ids) {
	}

	TableCatalogEntry &tableref;
	DataTable &table;
	std::vector<column_t> column_ids;

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::string ExtraRenderInformation() override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalTableScanOperatorState : public PhysicalOperatorState {
  public:
	PhysicalTableScanOperatorState(DataTable &table,
	                               ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(nullptr, parent_executor) {
		table.InitializeScan(scan_offset);
	}

	//! The current position in the scan
	ScanStructure scan_offset;
};
} // namespace duckdb
