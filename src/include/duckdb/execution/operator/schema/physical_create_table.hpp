//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

//! Physically CREATE TABLE statement
class PhysicalCreateTable : public PhysicalOperator {
public:
	PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info,
	                    idx_t estimated_cardinality);

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	unique_ptr<BoundCreateTableInfo> info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;
};
} // namespace duckdb
