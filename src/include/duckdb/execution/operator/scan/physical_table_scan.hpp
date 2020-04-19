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

namespace duckdb {

//! Represents a scan of a base table
class PhysicalTableScan : public PhysicalOperator {
public:
	PhysicalTableScan(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table, vector<column_t> column_ids,
	                  vector<unique_ptr<Expression>> filter, unordered_map<idx_t, vector<TableFilter>> table_filters);

	//! The table to scan
	TableCatalogEntry &tableref;
	//! The physical data table to scan
	DataTable &table;
	//! The column ids to project
	vector<column_t> column_ids;

	//! The filter expression
	unique_ptr<Expression> expression;
	//! Filters pushed down to table scan
	unordered_map<idx_t, vector<TableFilter>> table_filters;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	string ExtraRenderInformation() const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
