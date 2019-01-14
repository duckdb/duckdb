//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/schema/physical_create.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically CREATE TABLE statement
class PhysicalCreateTable : public PhysicalOperator {
public:
	PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<CreateTableInformation> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE, op.types), schema(schema), info(move(info)) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	unique_ptr<CreateTableInformation> info;
};
} // namespace duckdb
