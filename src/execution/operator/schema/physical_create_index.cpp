#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

PhysicalCreateIndex::PhysicalCreateIndex(LogicalOperator &op, TableCatalogEntry &table_p,
                                         const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                         vector<unique_ptr<Expression>> unbound_expressions,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table((DuckTableEntry &)table_p), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)) {
	D_ASSERT(table_p.IsDuckTable());
	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CreateIndexGlobalSinkState : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<Index> global_index;
};

class CreateIndexLocalSinkState : public LocalSinkState {
public:
	explicit CreateIndexLocalSinkState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};

	unique_ptr<Index> local_index;
	ArenaAllocator arena_allocator;
	vector<Key> keys;
	DataChunk key_chunk;
	vector<column_t> key_column_ids;
};

unique_ptr<GlobalSinkState> PhysicalCreateIndex::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CreateIndexGlobalSinkState>();

	// create the global index
	switch (info->index_type) {
	case IndexType::ART: {
		auto &storage = table.GetStorage();
		state->global_index = make_uniq<ART>(storage_ids, TableIOManager::Get(storage), unbound_expressions,
		                                       info->constraint_type, storage.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}
	return (std::move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateIndexLocalSinkState>(context.client);

	// create the local index
	switch (info->index_type) {
	case IndexType::ART: {
		auto &storage = table.GetStorage();
		state->local_index = make_uniq<ART>(storage_ids, TableIOManager::Get(storage), unbound_expressions,
		                                      info->constraint_type, storage.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}
	state->keys = vector<Key>(STANDARD_VECTOR_SIZE);
	state->key_chunk.Initialize(Allocator::Get(context.client), state->local_index->logical_types);

	for (idx_t i = 0; i < state->key_chunk.ColumnCount(); i++) {
		state->key_column_ids.push_back(i);
	}
	return std::move(state);
}

SinkResultType PhysicalCreateIndex::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                         DataChunk &input) const {

	D_ASSERT(input.ColumnCount() >= 2);
	auto &lstate = (CreateIndexLocalSinkState &)lstate_p;
	auto &row_identifiers = input.data[input.ColumnCount() - 1];

	// generate the keys for the given input
	lstate.key_chunk.ReferenceColumns(input, lstate.key_column_ids);
	lstate.arena_allocator.Reset();
	ART::GenerateKeys(lstate.arena_allocator, lstate.key_chunk, lstate.keys);

	auto &storage = table.GetStorage();
	auto art =
	    make_uniq<ART>(lstate.local_index->column_ids, lstate.local_index->table_io_manager,
	                     lstate.local_index->unbound_expressions, lstate.local_index->constraint_type, storage.db);
	if (!art->ConstructFromSorted(lstate.key_chunk.size(), lstate.keys, row_identifiers)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	// merge into the local ART
	if (!lstate.local_index->MergeIndexes(art.get())) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCreateIndex::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {

	auto &gstate = (CreateIndexGlobalSinkState &)gstate_p;
	auto &lstate = (CreateIndexLocalSinkState &)lstate_p;

	// merge the local index into the global index
	if (!gstate.global_index->MergeIndexes(lstate.local_index.get())) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
}

SinkFinalizeType PhysicalCreateIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {

	// here, we just set the resulting global index as the newly created index of the table

	auto &state = (CreateIndexGlobalSinkState &)gstate_p;
	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	auto &schema = *table.schema;
	auto index_entry = (DuckIndexEntry *)schema.CreateIndex(context, info.get(), &table);
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}

	index_entry->index = state.global_index.get();
	index_entry->info = storage.info;
	for (auto &parsed_expr : info->parsed_expressions) {
		index_entry->parsed_expressions.push_back(parsed_expr->Copy());
	}

	// vacuum excess memory
	state.global_index->Vacuum();

	// add index to storage
	storage.info->indexes.AddIndex(std::move(state.global_index));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

void PhysicalCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	// NOP
}

} // namespace duckdb
