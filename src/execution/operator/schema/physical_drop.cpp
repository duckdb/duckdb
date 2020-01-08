#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDrop::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	switch (info->type) {
	case CatalogType::SEQUENCE:
		if (info->schema == INVALID_SCHEMA) { // no temp sequences
			info->schema = DEFAULT_SCHEMA;
		}
		context.catalog.DropSequence(context.ActiveTransaction(), info.get());
		break;
	case CatalogType::VIEW:
		if (info->schema == INVALID_SCHEMA) { // no temp views
			info->schema = DEFAULT_SCHEMA;
		}
		context.catalog.DropView(context.ActiveTransaction(), info.get());
		break;
	case CatalogType::TABLE: {
		auto temp = context.temporary_objects->GetTableOrNull(context.ActiveTransaction(), info->name);
		if (temp && (info->schema == INVALID_SCHEMA || info->schema == TEMP_SCHEMA)) {
			context.temporary_objects->DropTable(context.ActiveTransaction(), info.get());
		} else {
			if (info->schema == INVALID_SCHEMA) {
				info->schema = DEFAULT_SCHEMA;
			}
			context.catalog.DropTable(context.ActiveTransaction(), info.get());
		}
		break;
	}
	case CatalogType::INDEX:
		if (info->schema == INVALID_SCHEMA) { // no temp views
			info->schema = DEFAULT_SCHEMA;
		}
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
