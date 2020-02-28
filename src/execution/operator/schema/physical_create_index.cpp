#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateIndex::CreateARTIndex() {
	auto art = make_unique<ART>(*table.storage, column_ids, move(unbound_expressions), info->unique);

	table.storage->AddIndex(move(art), expressions);
}

void PhysicalCreateIndex::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (column_ids.size() == 0) {
		throw NotImplementedException("CREATE INDEX does not refer to any columns in the base table!");
	}

	auto &schema = *table.schema;
	if (!schema.CreateIndex(context, info.get())) {
		// index already exists, but error ignored because of CREATE ... IF NOT
		// EXISTS
		return;
	}

	// create the chunk to hold intermediate expression results

	switch (info->index_type) {
	case IndexType::ART: {
		CreateARTIndex();
		break;
	}
	default:
		assert(0);
		throw NotImplementedException("Unimplemented index type");
	}

	chunk.SetCardinality(0);

	state->finished = true;
}
