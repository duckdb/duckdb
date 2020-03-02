#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateSchema::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	Catalog::GetCatalog(context).CreateSchema(context, info.get());
	state->finished = true;
}
