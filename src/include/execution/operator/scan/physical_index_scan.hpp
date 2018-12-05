//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/scan/physical_index_scan.hpp
//
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
	PhysicalIndexScan(LogicalOperator &op, TableCatalogEntry &tableref, DataTable &table,
	                  Index &index, vector<column_t> column_ids,
	                  unique_ptr<Expression> expression)
	    : PhysicalOperator(PhysicalOperatorType::INDEX_SCAN, op.types),
	      tableref(tableref), table(table), index(index),
	      column_ids(column_ids), expression(move(expression)) {
	}

	//! The table to scan
	TableCatalogEntry &tableref;
	//! The physical data table to scan
	DataTable &table;
	//! The index to use for the scan
	Index &index;
	//! The column ids to project
	vector<column_t> column_ids;
    //! The expression that must be fulfilled (i.e. the value looked up in the
    //! index)
    unique_ptr<Expression> expression;
	//! The expression type (e.g., >, <, >=, <=, =)
	ExpressionType expression_type;


	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	string ExtraRenderInformation() override;

	unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalIndexScanOperatorState : public PhysicalOperatorState {
  public:
	PhysicalIndexScanOperatorState(ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(nullptr, parent_executor), scan_state(nullptr) {
	}

	unique_ptr<IndexScanState> scan_state;
};
} // namespace duckdb
