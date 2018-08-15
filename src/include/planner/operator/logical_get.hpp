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
	LogicalGet() : LogicalOperator(LogicalOperatorType::GET) {}
	LogicalGet(std::shared_ptr<TableCatalogEntry> table, std::string alias, size_t table_index)
	    : LogicalOperator(LogicalOperatorType::GET), table(table),
	      alias(alias), table_index(table_index) { }

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! The base table to retrieve data from
	std::shared_ptr<TableCatalogEntry> table;
	std::string alias;
	//! The table index in the current bind context
	size_t table_index;
};
} // namespace duckdb