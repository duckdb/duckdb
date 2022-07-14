#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateIndexSourceState : public GlobalSourceState {
public:
	CreateIndexSourceState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCreateIndex::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CreateIndexSourceState>();
}

void PhysicalCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	auto &state = (CreateIndexSourceState &)gstate;
	if (state.finished) {
		return;
	}
	if (column_ids.empty()) {
		throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
	}

	auto &schema = *table.schema;
	auto index_entry = (IndexCatalogEntry *)schema.CreateIndex(context.client, info.get(), &table);
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return;
	}

	unique_ptr<Index> index;
	switch (info->index_type) {
	case IndexType::ART: {
		index = make_unique<ART>(column_ids, unbound_expressions, info->constraint_type, *context.client.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}
	index_entry->index = index.get();
	index_entry->info = table.storage->info;
	for (auto &parsed_expr : info->parsed_expressions) {
		index_entry->parsed_expressions.push_back(parsed_expr->Copy());
	}
	table.storage->AddIndex(move(index), expressions);

	chunk.SetCardinality(0);
	state.finished = true;
}

} // namespace duckdb
