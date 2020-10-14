#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_context.hpp"

using namespace std;

namespace duckdb {

void PhysicalDrop::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	switch (info->type) {
	case CatalogType::PREPARED_STATEMENT:
		if (!context.client.prepared_statements->DropEntry(context.client, info->name, false)) {
			// silently ignore
		}
		break;
	default:
		Catalog::GetCatalog(context.client).DropEntry(context.client, info.get());
		break;
	}
	state->finished = true;
}

} // namespace duckdb
