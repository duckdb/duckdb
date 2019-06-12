#include "execution/operator/schema/physical_create_index.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateIndex::CreateARTIndex() {
	auto art = make_unique<ART>(*table.storage, column_ids, move(unbound_expressions));

	DataChunk result;
	result.Initialize(art->types);

	DataTable::IndexScanState state;
	table.storage->InitializeIndexScan(state);

	DataChunk intermediate;
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	auto types = table.GetTypes(column_ids);
	intermediate.Initialize(types);

	// now we start incrementally building the index
	while (true) {
		intermediate.Reset();
		// scan a new chunk from the table to index
		table.storage->CreateIndexScan(state, column_ids, intermediate);
		if (intermediate.size() == 0) {
			// finished scanning for index creation
			// release all locks
			break;
		}
		// resolve the expressions for this chunk
		ExpressionExecutor executor(intermediate);
		executor.Execute(expressions, result);
		// insert into the index
		art->Insert(result, intermediate.data[intermediate.column_count - 1]);
	}
	table.storage->indexes.push_back(move(art));
}

void PhysicalCreateIndex::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (column_ids.size() == 0) {
		throw NotImplementedException("CREATE INDEX does not refer to any columns in the base table!");
	}

	auto &schema = *table.schema;
	if (!schema.CreateIndex(context.ActiveTransaction(), info.get())) {
		// index already exists, but error ignored because of CREATE ... IF NOT
		// EXISTS
		return;
	}

	// create the chunk to hold intermediate expression results
	// "Multidimensional indexes not supported yet"
	assert(expressions.size() == 1);

	switch (info->index_type) {
	case IndexType::ART: {
		CreateARTIndex();
		break;
	}
	default:
		assert(0);
		throw NotImplementedException("Unimplemented index type");
	}

	chunk.data[0].count = 0;

	state->finished = true;
}
