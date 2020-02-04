//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

//! PhysicalCreateSchema represents a CREATE SCHEMA command
class PhysicalCreateSchema : public PhysicalOperator {
public:
	PhysicalCreateSchema(unique_ptr<CreateSchemaInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SCHEMA, {TypeId::BOOL}), info(move(info)) {
	}

	unique_ptr<CreateSchemaInfo> info;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
};

} // namespace duckdb
