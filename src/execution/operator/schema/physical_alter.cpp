#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void PhysicalAlter::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const {
	auto &catalog = Catalog::GetCatalog(context.client);
	catalog.Alter(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
