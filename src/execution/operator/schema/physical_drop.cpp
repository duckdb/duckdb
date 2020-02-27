#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDrop::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	switch (info->type) {
	case CatalogType::PREPARED_STATEMENT:
		if (!context.prepared_statements->DropEntry(context.ActiveTransaction(), info->name, false)) {
			// silently ignore
		}
		break;
	default:
		context.catalog.DropEntry(context.ActiveTransaction(), info.get());
		break;
	}
	state->finished = true;
}
