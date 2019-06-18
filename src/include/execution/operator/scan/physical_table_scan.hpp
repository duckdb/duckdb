//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/scan/physical_table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "storage/data_table.hpp"

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
public:
	PhysicalTableScan(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table, vector<column_t> column_ids)
	    : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN, op.types), tableref(tableref), table(table),
	      column_ids(column_ids) {
	}

	//! The table to scan
	TableCatalogEntry &tableref;
	//! The physical data table to scan
	DataTable &table;
	//! The column ids to project
	vector<column_t> column_ids;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	string ExtraRenderInformation() const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalTableScanOperatorState : public PhysicalOperatorState {
public:
	PhysicalTableScanOperatorState(DataTable &table) : PhysicalOperatorState(nullptr) {
		table.InitializeScan(scan_offset);
	}

	//! The current position in the scan
	DataTable::ScanState scan_offset;
};
} // namespace duckdb
