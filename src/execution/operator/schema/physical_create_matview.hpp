#pragma once

#include "execution/operator/schema/physical_create_table_as.hpp"
#include "planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

//! Physically CREATE Materialized view AS statement
class PhysicalCreateMatView : public PhysicalCreateTableAs {
public:
	PhysicalCreateMatView(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info,
	                      idx_t estimated_cardinality);

	// Source interface
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
};
} // namespace duckdb
