#include "execution/operator/schema/physical_create_table.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateTable::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	int64_t inserted_count = 0;

	TableCatalogEntry* t_entry = nullptr;
	if (info->base->temporary) { // FIXME duplicated from schema_catalog_entry
		auto table = make_unique_base<CatalogEntry, TableCatalogEntry>(&context.catalog, nullptr, info.get());
		if (!context.temporary_tables->CreateEntry(context.ActiveTransaction(), info->base->table, move(table), info->dependencies)) {
			if (!info->base->if_not_exists) {
				throw CatalogException("Table or view with name \"%s\" already exists!", info->base->table.c_str());
			}
		}
		t_entry = (TableCatalogEntry*)context.temporary_tables->GetEntry(context.ActiveTransaction(), info->base->table);

	} else {
		schema->CreateTable(context.ActiveTransaction(), info.get());
		t_entry = schema->GetTable(context.ActiveTransaction(), info->base->table);
	}

	assert(t_entry);

	if (children.size() > 0) {
		while (true) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				break;
			}
			inserted_count += state->child_chunk.size();
			t_entry->storage->Append(*t_entry, context, state->child_chunk);
		}
	}

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(inserted_count));

	state->finished = true;
}
