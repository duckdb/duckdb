#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDrop::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	switch (info->type) {
	case CatalogType::SEQUENCE:
		context.catalog.DropSequence(context.ActiveTransaction(), info.get());
		break;
	case CatalogType::VIEW:
		context.catalog.DropView(context.ActiveTransaction(), info.get());
		break;
	case CatalogType::TABLE: {
		context.catalog.DropTable(context.ActiveTransaction(), info.get());
		break;
	}
	case CatalogType::INDEX:
		context.catalog.DropIndex(context.ActiveTransaction(), info.get());
		break;
	case CatalogType::SCHEMA:
		context.catalog.DropSchema(context.ActiveTransaction(), info.get());
		break;
	case CatalogType::PREPARED_STATEMENT:
		if (!context.prepared_statements->DropEntry(context.ActiveTransaction(), info->name, false)) {
			// silently ignore
		}
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for drop statement");
	}
	state->finished = true;
}
