#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalAlter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	context.catalog.AlterTable(context, (AlterTableInfo *)info.get());
	state->finished = true;
}
