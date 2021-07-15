#include "duckdb/execution/operator/schema/physical_create_enum.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void PhysicalCreateEnum::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                          PhysicalOperatorState *state) const {
	Catalog::GetCatalog(context.client).CreateEnum(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
