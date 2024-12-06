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

	// Convert the virtual column ids to physical column ids.
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CreateARTIndexGlobalSinkState : public GlobalSinkState {
public:
	unique_ptr<BoundIndex> global_index;
};

class CreateARTIndexLocalSinkState : public LocalSinkState {
public:
	explicit CreateARTIndexLocalSinkState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};

	unique_ptr<BoundIndex> local_index;
	ArenaAllocator arena_allocator;

	DataChunk key_chunk;
	unsafe_vector<ARTKey> keys;
	vector<column_t> key_column_ids;

	DataChunk row_id_chunk;
	unsafe_vector<ARTKey> row_ids;
};

unique_ptr<GlobalSinkState> PhysicalCreateARTIndex::GetGlobalSinkState(ClientContext &context) const {
	// Create the global sink state and add the global index.
	auto state = make_uniq<CreateARTIndexGlobalSinkState>();
	auto &storage = table.GetStorage();
	state->global_index = make_uniq<ART>(info->index_name, info->constraint_type, storage_ids,
	                                     TableIOManager::Get(storage), unbound_expressions, storage.db);
	return (std::move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateARTIndex::GetLocalSinkState(ExecutionContext &context) const {
	// Create the local sink state and add the local index.
	auto state = make_uniq<CreateARTIndexLocalSinkState>(context.client);
	auto &storage = table.GetStorage();
	state->local_index = make_uniq<ART>(info->index_name, info->constraint_type, storage_ids,
	                                    TableIOManager::Get(storage), unbound_expressions, storage.db);

	// Initialize the local sink state.
	state->keys.resize(STANDARD_VECTOR_SIZE);
	state->row_ids.resize(STANDARD_VECTOR_SIZE);
	state->key_chunk.Initialize(Allocator::Get(context.client), state->local_index->logical_types);
	state->row_id_chunk.Initialize(Allocator::Get(context.client), vector<LogicalType> {LogicalType::ROW_TYPE});
	for (idx_t i = 0; i < state->key_chunk.ColumnCount(); i++) {
		state->key_column_ids.push_back(i);
	}
	return std::move(state);
}

SinkResultType PhysicalCreateARTIndex::SinkUnsorted(OperatorSinkInput &input) const {

	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();
	auto row_count = l_state.key_chunk.size();

	// Insert each key and its corresponding row ID.
	auto &art = l_state.local_index->Cast<ART>();
	for (idx_t i = 0; i < row_count; i++) {
		if (!art.Insert(art.tree, l_state.keys[i], 0, l_state.row_ids[i], art.tree.GetGateStatus())) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalCreateARTIndex::SinkSorted(OperatorSinkInput &input) const {

	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();
	auto &storage = table.GetStorage();
	auto &l_index = l_state.local_index;

	// Construct an ART for this chunk.
	auto art = make_uniq<ART>(info->index_name, l_index->GetConstraintType(), l_index->GetColumnIds(),
	                          l_index->table_io_manager, l_index->unbound_expressions, storage.db,
	                          l_index->Cast<ART>().allocators);
	if (!art->Construct(l_state.keys, l_state.row_ids, l_state.key_chunk.size())) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	// Merge the ART into the local ART.
	if (!l_index->MergeIndexes(*art)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalCreateARTIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {

	D_ASSERT(chunk.ColumnCount() >= 2);
	auto &l_state = input.local_state.Cast<CreateARTIndexLocalSinkState>();
	l_state.arena_allocator.Reset();
	l_state.key_chunk.ReferenceColumns(chunk, l_state.key_column_ids);
	ART::GenerateKeyVectors(l_state.arena_allocator, l_state.key_chunk, chunk.data[chunk.ColumnCount() - 1],
	                        l_state.keys, l_state.row_ids);

	if (sorted) {
		return SinkSorted(input);
	}
	return SinkUnsorted(input);
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
	state.global_index->VerifyAllocations();

	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	auto &schema = table.schema;
	info->column_ids = storage_ids;

	// Ensure that the index does not yet exist.
	// FIXME: We should early-out prior to creating the index.
	if (schema.GetEntry(schema.GetCatalogTransaction(context), CatalogType::INDEX_ENTRY, info->index_name)) {
		if (info->on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
			throw CatalogException("Index with name \"%s\" already exists!", info->index_name);
		}
		// IF NOT EXISTS on existing index. We are done.
		return SinkFinalizeType::READY;
	}

	auto index_entry = schema.CreateIndex(schema.GetCatalogTransaction(context), *info, table).get();
	D_ASSERT(index_entry);
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
