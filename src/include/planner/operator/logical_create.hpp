//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_create.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreate : public LogicalOperator {
  public:
	LogicalCreate(SchemaCatalogEntry *schema, std::string table,
	              std::vector<ColumnDefinition> columns,
	              std::vector<std::unique_ptr<Constraint>> constraints)
	    : LogicalOperator(LogicalOperatorType::CREATE), schema(schema),
	      table(table), columns(columns), constraints(move(constraints)) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	std::string table;
	//! List of columns of the table
	std::vector<ColumnDefinition> columns;
	//! List of constraints on the table
	std::vector<std::unique_ptr<Constraint>> constraints;
};
} // namespace duckdb
