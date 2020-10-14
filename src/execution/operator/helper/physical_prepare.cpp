#include "duckdb/execution/operator/helper/physical_prepare.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/prepared_statement_catalog_entry.hpp"

using namespace std;

namespace duckdb {

void PhysicalPrepare::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &client = context.client;
	// create the catalog entry
	auto entry = make_unique<PreparedStatementCatalogEntry>(name, move(prepared));
	entry->catalog = &client.catalog;

	// now store plan in context
	auto &dependencies = entry->prepared->dependencies;
	if (!client.prepared_statements->CreateEntry(client, name, move(entry), dependencies)) {
		throw Exception("Failed to prepare statement");
	}
	state->finished = true;
}

} // namespace duckdb
