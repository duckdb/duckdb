//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/schema/physical_create_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! Physically CREATE INDEX statement
class PhysicalCreateView : public PhysicalOperator {
public:
	PhysicalCreateView(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<CreateViewInformation> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types), schema(schema), info(std::move(info)) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! The schema to create the view in
	SchemaCatalogEntry *schema;
	// Info for index creation
	unique_ptr<CreateViewInformation> info;
};
} // namespace duckdb
