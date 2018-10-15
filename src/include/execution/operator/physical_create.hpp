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
	PhysicalCreate(SchemaCatalogEntry *schema,
	               std::unique_ptr<CreateTableInformation> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE), schema(schema),
	      info(move(info)) {}

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;
	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	std::unique_ptr<CreateTableInformation> info;
};
} // namespace duckdb
