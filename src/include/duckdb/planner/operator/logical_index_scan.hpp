//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_index_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

//! LogicalIndex represents an Index Scan operation
class LogicalIndexScan : public LogicalOperator {
public:
	LogicalIndexScan(TableCatalogEntry &tableref, DataTable &table, Index &index, vector<column_t> column_ids,
	                 idx_t table_index)
	    : LogicalOperator(LogicalOperatorType::INDEX_SCAN), tableref(tableref), table(table), index(index),
	      column_ids(column_ids), table_index(table_index) {
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

	//! The table index in the current bind context
	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, column_ids.size());
	}

protected:
	void ResolveTypes() override {
		if (column_ids.size() == 0) {
			types = {TypeId::INT32};
		} else {
			types = tableref.GetTypes(column_ids);
		}
	}
};

} // namespace duckdb
