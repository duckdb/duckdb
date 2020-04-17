//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
public:
	LogicalInsert(TableCatalogEntry *table) : LogicalOperator(LogicalOperatorType::INSERT), table(table) {
	}

	vector<vector<unique_ptr<Expression>>> insert_values;
	//! The insertion map ([table_index -> index in result, or INVALID_INDEX if not specified])
	vector<idx_t> column_index_map;
	//! The expected types for the INSERT statement (obtained from the column types)
	vector<SQLType> expected_types;
	//! The base table to insert into
	TableCatalogEntry *table;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::INT64);
	}
};
} // namespace duckdb
