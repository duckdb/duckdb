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
	LogicalInsert(std::shared_ptr<TableCatalogEntry> table,
	              std::vector<std::unique_ptr<AbstractExpression>> value_list)
	    : LogicalOperator(LogicalOperatorType::INSERT),
	      value_list(move(value_list)), table(table) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! The set of values to insert into the table
	std::vector<std::unique_ptr<AbstractExpression>> value_list;
	//! The base table to insert into
	std::shared_ptr<TableCatalogEntry> table;
};
} // namespace duckdb
