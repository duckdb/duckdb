//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/schema/physical_create.hpp
//
//
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
	      info(move(info)) {
	}

	std::vector<TypeId> GetTypes() override;
	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	std::unique_ptr<CreateTableInformation> info;
};
} // namespace duckdb
