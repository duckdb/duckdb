#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void PhysicalCreateSchema::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                            PhysicalOperatorState *state) const {
	Catalog::GetCatalog(context.client).CreateSchema(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
