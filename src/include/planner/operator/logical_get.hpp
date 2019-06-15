//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
public:
	LogicalGet() : LogicalOperator(LogicalOperatorType::GET), table(nullptr) {
	}
	LogicalGet(TableCatalogEntry *table, index_t table_index, vector<column_t> column_ids)
	    : LogicalOperator(LogicalOperatorType::GET), table(table), table_index(table_index), column_ids(column_ids) {
	}

	index_t EstimateCardinality() override;

	//! The base table to retrieve data from
	TableCatalogEntry *table;
	//! The table index in the current bind context
	index_t table_index;
	//! Bound column IDs
	vector<column_t> column_ids;

	string ParamsToString() const override {
		if (!table) {
			return "";
		}
		return "(" + table->name + ")";
	}

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
