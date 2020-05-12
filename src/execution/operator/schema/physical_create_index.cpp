#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace std;

namespace duckdb {

void PhysicalCreateIndex::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	if (column_ids.size() == 0) {
		throw NotImplementedException("CREATE INDEX does not refer to any columns in the base table!");
	}

	auto &schema = *table.schema;
	auto index_entry = (IndexCatalogEntry *)schema.CreateIndex(context, info.get());
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return;
	}

	unique_ptr<Index> index;
	switch (info->index_type) {
	case IndexType::ART: {
		index = make_unique<ART>(column_ids, move(unbound_expressions), info->unique);
		break;
	}
	default:
		assert(0);
		throw NotImplementedException("Unimplemented index type");
	}
	index_entry->index = index.get();
	index_entry->info = table.storage->info;
	table.storage->AddIndex(move(index), expressions);

	chunk.SetCardinality(0);
	state->finished = true;
}

} // namespace duckdb
