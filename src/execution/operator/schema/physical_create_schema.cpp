#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace std;

namespace duckdb {

void PhysicalCreateSchema::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	Catalog::GetCatalog(context.client).CreateSchema(context.client, info.get());
	state->finished = true;
}

} // namespace duckdb
