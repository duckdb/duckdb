//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operation/physical_create.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include <fstream>

namespace duckdb {

//! Physically CREATE TABLE statement
class PhysicalCreate : public PhysicalOperator {
  public:
	PhysicalCreate(SchemaCatalogEntry *schema, std::string table,
	               std::vector<ColumnDefinition> columns,
	               std::vector<std::unique_ptr<Constraint>> constraints)
	    : PhysicalOperator(PhysicalOperatorType::CREATE), schema(schema),
	      table(table), columns(columns), constraints(move(constraints)) {}

	std::vector<TypeId> GetTypes() override;
	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

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
