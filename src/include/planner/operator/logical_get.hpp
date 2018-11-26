//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_get.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
  public:
	LogicalGet() : LogicalOperator(LogicalOperatorType::GET), table(nullptr) {
	}
	LogicalGet(TableCatalogEntry *table, size_t table_index,
	           std::vector<column_t> column_ids)
	    : LogicalOperator(LogicalOperatorType::GET), table(table),
	      table_index(table_index), column_ids(column_ids) {
		referenced_tables.insert(table_index);
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	//! The base table to retrieve data from
	TableCatalogEntry *table;
	//! The table index in the current bind context
	size_t table_index;
	//! Bound column IDs
	std::vector<column_t> column_ids;

	std::string ParamsToString() const override {
		if (!table) {
			return "";
		}
		return "(" + table->name + ")";
	}
};
} // namespace duckdb
