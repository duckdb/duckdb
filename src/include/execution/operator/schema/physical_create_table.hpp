//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/schema/physical_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

//! Physically CREATE TABLE statement
class PhysicalCreateTable : public PhysicalOperator {
public:
	PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE, op.types), schema(schema), info(move(info)) {
	}

	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	unique_ptr<BoundCreateTableInfo> info;
};
} // namespace duckdb
