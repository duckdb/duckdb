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
	LogicalInsert(TableCatalogEntry *table) : LogicalOperator(LogicalOperatorType::INSERT), table(table) {
	}

	vector<vector<unique_ptr<Expression>>> insert_values;
	vector<int> column_index_map;

	//! The base table to insert into
	TableCatalogEntry *table;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
