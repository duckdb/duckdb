#include "duckdb/execution/operator/schema/physical_create_function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {

void PhysicalCreateFunction::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                              PhysicalOperatorState *state) const {
	Catalog::GetCatalog(context.client).CreateFunction(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
