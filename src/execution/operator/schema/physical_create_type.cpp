#include "duckdb/execution/operator/schema/physical_create_type.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void PhysicalCreateType::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                          PhysicalOperatorState *state) const {
	Catalog::GetCatalog(context.client).CreateType(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
