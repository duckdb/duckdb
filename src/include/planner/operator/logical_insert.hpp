//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_insert.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
  public:
	LogicalInsert(TableCatalogEntry *table,
	              std::vector<std::unique_ptr<AbstractExpression>> value_list)
	    : LogicalOperator(LogicalOperatorType::INSERT, std::move(value_list)),
	      table(table) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! The base table to insert into
	TableCatalogEntry *table;
};
} // namespace duckdb
