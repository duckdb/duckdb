#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

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
	explicit CreateIndexLocalSinkState(ExpressionExecutor executor) : executor(move(executor)) {
	}

	//! Local indexes build from chunks of the scanned data
	unique_ptr<Index> local_index;
	//! Chunk containing the ART keys
	DataChunk key_chunk;
	//! To execute on the input chunk
	ExpressionExecutor executor;
};

unique_ptr<GlobalSinkState> PhysicalCreateIndex::GetGlobalSinkState(ClientContext &context) const {

	auto state = make_unique<CreateIndexGlobalSinkState>();

	// create the global index
	switch (info->index_type) {
	case IndexType::ART: {
		state->global_index = make_unique<ART>(storage_ids, unbound_expressions, info->constraint_type, *context.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	return (move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateIndex::GetLocalSinkState(ExecutionContext &context) const {

	auto &allocator = Allocator::Get(table.storage->db);
	ExpressionExecutor executor(allocator, expressions);
	auto state = make_unique<CreateIndexLocalSinkState>(move(executor));

	// create the local index
	switch (info->index_type) {
	case IndexType::ART: {
		state->local_index =
		    make_unique<ART>(storage_ids, unbound_expressions, info->constraint_type, *context.client.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	state->key_chunk.Initialize(allocator, state->local_index->logical_types);
	return move(state);
}

SinkResultType PhysicalCreateIndex::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                         DataChunk &input) const {

	auto &lstate = (CreateIndexLocalSinkState &)lstate_p;

	// resolve the expressions for this chunk
	lstate.key_chunk.Reset();
	lstate.executor.Execute(input, lstate.key_chunk);

	// scan the sorted row data and construct the index from it
	{
		IndexLock local_lock;
		lstate.local_index->InitializeLock(local_lock);
		if (!lstate.local_index->Insert(local_lock, lstate.key_chunk, input.data[input.ColumnCount() - 1], true)) {
			throw ConstraintException("Can't create unique index, duplicate data on indexed column(s)");
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCreateIndex::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {

	auto &gstate = (CreateIndexGlobalSinkState &)gstate_p;
	auto &lstate = (CreateIndexLocalSinkState &)lstate_p;

	// merge the local index into the global index
	gstate.global_index->MergeIndexes(lstate.local_index.get());
}

SinkFinalizeType PhysicalCreateIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {

	// here, we just set the resulting global index as the newly created index of the table
	auto &state = (CreateIndexGlobalSinkState &)gstate_p;

	if (!table.storage->IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	auto &schema = *table.schema;
	auto index_entry = (IndexCatalogEntry *)schema.CreateIndex(context, info.get(), &table);
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}

	index_entry->index = state.global_index.get();
	index_entry->info = table.storage->info;
	for (auto &parsed_expr : info->parsed_expressions) {
		index_entry->parsed_expressions.push_back(parsed_expr->Copy());
	}

	table.storage->info->indexes.AddIndex(move(state.global_index));
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
