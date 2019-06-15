//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/scan/physical_index_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "storage/data_table.hpp"
#include "storage/index.hpp"

namespace duckdb {

//! Represents a scan of an index
class PhysicalIndexScan : public PhysicalOperator {
public:
	PhysicalIndexScan(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table, Index &index,
	                  vector<column_t> column_ids)
	    : PhysicalOperator(PhysicalOperatorType::INDEX_SCAN, op.types), tableref(tableref), table(table), index(index),
	      column_ids(column_ids) {
	}

	//! The table to scan
	TableCatalogEntry &tableref;
	//! The physical data table to scan
	DataTable &table;
	//! The index to use for the scan
	Index &index;
	//! The column ids to project
	vector<column_t> column_ids;

	//! The value for the query predicate
	Value low_value;
	Value high_value;
	Value equal_value;

	//! If the predicate is low, high or equal
	bool low_index = false;
	bool high_index = false;
	bool equal_index = false;

	//! The expression type (e.g., >, <, >=, <=)
	ExpressionType low_expression_type;
	ExpressionType high_expression_type;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	string ExtraRenderInformation() const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalIndexScanOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexScanOperatorState() : PhysicalOperatorState(nullptr), scan_state(nullptr) {
	}

	unique_ptr<IndexScanState> scan_state;
};
} // namespace duckdb
