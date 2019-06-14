//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
public:
	LogicalInsert(TableCatalogEntry *table, vector<unique_ptr<Expression>> bound_defaults)
	    : LogicalOperator(LogicalOperatorType::INSERT), table(table), bound_defaults(move(bound_defaults)) {
	}

	vector<vector<unique_ptr<Expression>>> insert_values;
	vector<index_t> column_index_map;

	//! The base table to insert into
	TableCatalogEntry *table;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
