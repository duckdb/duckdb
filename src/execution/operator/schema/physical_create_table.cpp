#include "duckdb/execution/operator/schema/physical_create_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateTable::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	int64_t inserted_count = 0;

	auto table = (TableCatalogEntry *)schema->CreateTable(context, info.get());
	if (table && children.size() > 0) {
		while (true) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
			inserted_count += state->child_chunk.size();
			table->storage->Append(*table, context, state->child_chunk);
		}
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(inserted_count));
	}

	state->finished = true;
}
