#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/main/database_manager.hpp"

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
	explicit CreateIndexLocalSinkState(const vector<unique_ptr<Expression>> &expressions) : executor(expressions) {
	}

	//! Local indexes build from chunks of the scanned data
	unique_ptr<Index> local_index;

	DataChunk key_chunk;
	unique_ptr<GlobalSortState> global_sort_state;
	LocalSortState local_sort_state;

	RowLayout payload_layout;
	vector<LogicalType> payload_types;

	ExpressionExecutor executor;
};

unique_ptr<GlobalSinkState> PhysicalCreateIndex::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<CreateIndexGlobalSinkState>();

	// create the global index
	switch (info->index_type) {
	case IndexType::ART: {
		state->global_index = make_unique<ART>(storage_ids, TableIOManager::Get(*table.storage), unbound_expressions,
		                                       info->constraint_type, table.storage->db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	return (move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(table.storage->db);

	auto state = make_unique<CreateIndexLocalSinkState>(expressions);

	// create the local index
	switch (info->index_type) {
	case IndexType::ART: {
		state->local_index = make_unique<ART>(storage_ids, TableIOManager::Get(*table.storage), unbound_expressions,
		                                      info->constraint_type, table.storage->db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	state->key_chunk.Initialize(allocator, state->local_index->logical_types);

	// ordering of the entries of the index
	vector<BoundOrderByNode> orders;
	for (idx_t i = 0; i < state->local_index->logical_types.size(); i++) {
		auto col_expr = make_unique_base<Expression, BoundReferenceExpression>(state->local_index->logical_types[i], i);
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(col_expr));
	}

	// row layout of the global sort state
	state->payload_types = state->local_index->logical_types;
	state->payload_types.emplace_back(LogicalType::ROW_TYPE);
	state->payload_layout.Initialize(state->payload_types);

	// initialize global and local sort state
	auto &buffer_manager = BufferManager::GetBufferManager(table.storage->db);
	state->global_sort_state = make_unique<GlobalSortState>(buffer_manager, orders, state->payload_layout);
	state->local_sort_state.Initialize(*state->global_sort_state, buffer_manager);

	return move(state);
}

SinkResultType PhysicalCreateIndex::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                         DataChunk &input) const {

	// here, we sink all incoming data into the local_sink_state after executing the expression executor
	auto &lstate = (CreateIndexLocalSinkState &)lstate_p;

	// resolve the expressions for this chunk
	D_ASSERT(!lstate.executor.HasContext());
	lstate.key_chunk.Reset();
	lstate.executor.Execute(input, lstate.key_chunk);

	// create the payload chunk
	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty(lstate.payload_types);
	for (idx_t i = 0; i < lstate.local_index->logical_types.size(); i++) {
		payload_chunk.data[i].Reference(lstate.key_chunk.data[i]);
	}
	payload_chunk.data[lstate.payload_types.size() - 1].Reference(input.data[input.ColumnCount() - 1]);
	payload_chunk.SetCardinality(input.size());

	// sink the chunks into the local sort state
	lstate.local_sort_state.SinkChunk(lstate.key_chunk, payload_chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCreateIndex::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {

	// here, we take the sunk data chunks and sort them
	// then, we scan the sorted data and build a local index from it
	// finally, we merge the local index into the global index

	auto &gstate = (CreateIndexGlobalSinkState &)gstate_p;
	auto &lstate = (CreateIndexLocalSinkState &)lstate_p;

	auto &allocator = Allocator::Get(table.storage->db);

	// add local state to global state, which sorts the data
	lstate.global_sort_state->AddLocalState(lstate.local_sort_state);
	lstate.global_sort_state->PrepareMergePhase();

	// scan the sorted row data and construct the index from it
	{
		IndexLock local_lock;
		lstate.local_index->InitializeLock(local_lock);
		if (!lstate.global_sort_state->sorted_blocks.empty()) {
			PayloadScanner scanner(*lstate.global_sort_state->sorted_blocks[0]->payload_data,
			                       *lstate.global_sort_state);
			lstate.local_index->ConstructAndMerge(local_lock, scanner, allocator);
		}
	}

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
