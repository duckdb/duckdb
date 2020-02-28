#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateSchema::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	context.catalog.CreateSchema(context, info.get());
	state->finished = true;
}
