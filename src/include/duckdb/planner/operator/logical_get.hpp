//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
public:
	LogicalGet(idx_t table_index);
	LogicalGet(TableCatalogEntry *table, idx_t table_index, vector<column_t> column_ids);

	idx_t EstimateCardinality() override;

	//! The base table to retrieve data from
	TableCatalogEntry *table;
	//! The table index in the current bind context
	idx_t table_index;
	//! Bound column IDs
	vector<column_t> column_ids;

	string ParamsToString() const override;

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
