#include "duckdb/execution/operator/schema/physical_create_art_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

namespace duckdb {

PhysicalCreateARTIndex::PhysicalCreateARTIndex(LogicalOperator &op, TableCatalogEntry &table_p,
                                               const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                               vector<unique_ptr<Expression>> unbound_expressions,
                                               idx_t estimated_cardinality, const bool sorted)
    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)),
      sorted(sorted) {

	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CreateARTIndexGlobalSinkState : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<BoundIndex> global_index;
};

class CreateARTIndexLocalSinkState : public LocalSinkState {
public:
	explicit CreateARTIndexLocalSinkState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};

	unique_ptr<BoundIndex> local_index;
	ArenaAllocator arena_allocator;
	vector<ARTKey> keys;
	DataChunk key_chunk;
	vector<column_t> key_column_ids;
};

unique_ptr<GlobalSinkState> PhysicalCreateARTIndex::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CreateARTIndexGlobalSinkState>();

	// create the global index
	auto &storage = table.GetStorage();
	state->global_index = make_uniq<ART>(info->index_name, info->constraint_type, storage_ids,
	                                     TableIOManager::Get(storage), unbound_expressions, storage.db);

	return (std::move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateARTIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateARTIndexLocalSinkState>(context.client);

	// create the local index

	auto &storage = table.GetStorage();
	state->local_index = make_uniq<ART>(info->index_name, info->constraint_type, storage_ids,
	                                    TableIOManager::Get(storage), unbound_expressions, storage.db);

	state->keys = vector<ARTKey>(STANDARD_VECTOR_SIZE);
	state->key_chunk.Initialize(Allocator::Get(context.client), state->local_index->logical_types);

	for (idx_t i = 0; i < state->key_chunk.ColumnCount(); i++) {
		state->key_column_ids.push_back(i);
	}
	return std::move(state);
}

SinkResultType PhysicalCreateARTIndex::SinkUnsorted(Vector &row_identifiers, OperatorSinkInput &input) const {

	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();
	auto count = l_state.key_chunk.size();

	UnifiedVectorFormat row_id_data;
	row_identifiers.ToUnifiedFormat(count, row_id_data);
	auto row_ids = UnifiedVectorFormat::GetData<row_t>(row_id_data);

	// insert the row IDs
	auto &art = l_state.local_index->Cast<ART>();
	for (idx_t i = 0; i < count; i++) {
		if (!art.Insert(art.tree, l_state.keys[i], 0, row_ids[i])) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalCreateARTIndex::SinkSorted(Vector &row_identifiers, OperatorSinkInput &input) const {

	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();
	auto &storage = table.GetStorage();
	auto &l_index = l_state.local_index;

	// create an ART from the chunk
	auto art = make_uniq<ART>(info->index_name, l_index->GetConstraintType(), l_index->GetColumnIds(),
	                          l_index->table_io_manager, l_index->unbound_expressions, storage.db,
	                          l_index->Cast<ART>().allocators);
	if (!art->ConstructFromSorted(l_state.key_chunk.size(), l_state.keys, row_identifiers)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	// merge into the local ART
	if (!l_index->MergeIndexes(*art)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalCreateARTIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {

	D_ASSERT(chunk.ColumnCount() >= 2);
	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();
	l_state.key_chunk.ReferenceColumns(chunk, l_state.key_column_ids);
	l_state.arena_allocator.Reset();

	// Insert the keys and their corresponding row identifiers.
	auto &row_identifiers = chunk.data[chunk.ColumnCount() - 1];
	if (sorted) {
		ART::GenerateKeys<true>(l_state.arena_allocator, l_state.key_chunk, l_state.keys);
		return SinkSorted(row_identifiers, input);
	}

	ART::GenerateKeys(l_state.arena_allocator, l_state.key_chunk, l_state.keys);
	return SinkUnsorted(row_identifiers, input);
}

SinkCombineResultType PhysicalCreateARTIndex::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {

	auto &g_state = input.global_state.Cast<CreateARTIndexGlobalSinkState>();
	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();

	// merge the local index into the global index
	if (!g_state.global_index->MergeIndexes(*l_state.local_index)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCreateARTIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {

	// here, we set the resulting global index as the newly created index of the table
	auto &state = input.global_state.Cast<CreateARTIndexGlobalSinkState>();

	// vacuum excess memory and verify
	state.global_index->Vacuum();
	D_ASSERT(!state.global_index->VerifyAndToString(true).empty());

	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	auto &schema = table.schema;
	info->column_ids = storage_ids;
	auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(context), *info, table).get();
	if (!index_entry) {
		D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}
	auto &index = index_entry->Cast<DuckIndexEntry>();
	index.initial_index_size = state.global_index->GetInMemorySize();

	// add index to storage
	storage.AddIndex(std::move(state.global_index));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalCreateARTIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
