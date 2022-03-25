//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalUpdate : public LogicalOperator {
public:
	explicit LogicalUpdate(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE), table(table), table_index(0), return_chunk(false) {
	}

	//! The base table to update
	TableCatalogEntry *table;
	//! table catalog index
	idx_t table_index;
	//! if returning option is used, return the update chunk
	bool return_chunk;
	vector<column_t> columns;
	vector<unique_ptr<Expression>> bound_defaults;
	bool update_is_del_and_insert;

protected:
	vector<ColumnBinding> GetColumnBindings() override {
		if (return_chunk) {
			return GenerateColumnBindings(table_index, table->columns.size());
		}
		return {ColumnBinding(0, 0)};
	}

	void ResolveTypes() override {
		if (return_chunk) {
			types = table->GetTypes();
		} else {
			types.emplace_back(LogicalType::BIGINT);
		}
	}
};
} // namespace duckdb
