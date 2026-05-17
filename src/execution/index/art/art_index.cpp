#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/art_operator.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {
namespace {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
class ARTBuildBindData : public IndexBuildBindData {
public:
	bool sorted = false;
	bool has_deletes = false;
};

unique_ptr<IndexBuildBindData> ARTBuildBind(IndexBuildBindInput &input) {
	auto bind_data = make_uniq<ARTBuildBindData>();

	// TODO: Verify that the the ART is applicable for the given columns and types.

	// We used to not sort for VARCHAR and multi-column indexes with the old sort implementation
	// The new sorting implementation handles these cases much better and sorting improves performance now
	bind_data->sorted = true;
	if (input.info.constraint_type == IndexConstraintType::UNIQUE ||
	    input.info.constraint_type == IndexConstraintType::PRIMARY) {
		auto &transaction = DuckTransaction::Get(input.context, input.table.catalog);
		bind_data->has_deletes = transaction.GetUndoProperties().has_deletes;
	}

	return std::move(bind_data);
}

bool ARTBuildSort(IndexBuildSortInput &input) {
	auto &bind_data = input.bind_data->Cast<ARTBuildBindData>();
	return bind_data.sorted;
}

//----------------------------------------------------------------------------------------------------------------------
// Global State
//----------------------------------------------------------------------------------------------------------------------
class ARTBuildGlobalState : public IndexBuildGlobalState {
public:
	unique_ptr<BoundIndex> global_index;
	unique_ptr<BoundIndex> delete_index;
	bool has_delete_index = false;
};

unique_ptr<IndexBuildGlobalState> ARTBuildGlobalInit(IndexBuildInitGlobalStateInput &input) {
	auto state = make_uniq<ARTBuildGlobalState>();

	auto &storage = input.table.GetStorage();
	state->global_index = make_uniq<ART>(input.info.index_name, input.info.constraint_type, input.storage_ids,
	                                     TableIOManager::Get(storage), input.expressions, storage.db);
	auto &bind_data = input.bind_data->Cast<ARTBuildBindData>();
	if (bind_data.has_deletes) {
		state->delete_index = state->global_index->Cast<ART>().CreateDeltaIndex(DeltaIndexType::LOCAL_DELETE);
	}

	return std::move(state);
}

//----------------------------------------------------------------------------------------------------------------------
// Local State
//----------------------------------------------------------------------------------------------------------------------
class ARTBuildLocalState : public IndexBuildLocalState {
public:
	unique_ptr<BoundIndex> local_index;
	unique_ptr<BoundIndex> delete_index;
	ArenaAllocator arena_allocator;
	reference<DuckTransaction> transaction;
	bool has_delete_index = false;

	unsafe_vector<ARTKey> keys;
	unsafe_vector<ARTKey> row_ids;
	unsafe_vector<ARTKey> visible_keys;
	unsafe_vector<ARTKey> visible_row_ids;
	unsafe_vector<ARTKey> deleted_keys;
	unsafe_vector<ARTKey> deleted_row_ids;

	ARTBuildLocalState(ClientContext &context, DuckTableEntry &table)
	    : arena_allocator(Allocator::Get(context)), transaction(DuckTransaction::Get(context, table.catalog)) {};
};

unique_ptr<IndexBuildLocalState> ARTBuildLocalInit(IndexBuildInitLocalStateInput &input) {
	// Create the local sink state and add the local index.
	auto state = make_uniq<ARTBuildLocalState>(input.context, input.table);

	auto &storage = input.table.GetStorage();
	state->local_index = make_uniq<ART>(input.info.index_name, input.info.constraint_type, input.storage_ids,
	                                    TableIOManager::Get(storage), input.expressions, storage.db);
	auto &bind_data = input.bind_data->Cast<ARTBuildBindData>();
	if (bind_data.has_deletes) {
		state->delete_index = state->local_index->Cast<ART>().CreateDeltaIndex(DeltaIndexType::LOCAL_DELETE);
	}

	// Initialize the local sink state.
	state->keys.resize(STANDARD_VECTOR_SIZE);
	state->row_ids.resize(STANDARD_VECTOR_SIZE);

	return std::move(state);
}

//----------------------------------------------------------------------------------------------------------------------
// Sink
//----------------------------------------------------------------------------------------------------------------------
void ARTBuildSinkUnsorted(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk,
                          unsafe_vector<ARTKey> &keys, unsafe_vector<ARTKey> &row_ids, const idx_t row_count) {
	auto &l_state = input.local_state.Cast<ARTBuildLocalState>();
	auto &art = l_state.local_index->Cast<ART>();

	// Insert each key and its corresponding row ID.
	for (idx_t i = 0; i < row_count; i++) {
		auto status = art.tree.GetGateStatus();
		auto conflict_type = ARTOperator::Insert(l_state.arena_allocator, art, art.tree, keys[i], 0, row_ids[i], status,
		                                         DeleteIndexInfo(), IndexAppendMode::DEFAULT);
		D_ASSERT(conflict_type != ARTConflictType::TRANSACTION);
		if (conflict_type == ARTConflictType::CONSTRAINT) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}
	}
}

void ARTBuildSinkSorted(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk,
                        unsafe_vector<ARTKey> &keys, unsafe_vector<ARTKey> &row_ids, const idx_t row_count) {
	auto &l_state = input.local_state.Cast<ARTBuildLocalState>();
	auto &storage = input.table.GetStorage();
	auto &l_index = l_state.local_index;

	// Construct an ART for this chunk.
	auto art = make_uniq<ART>(input.info.index_name, l_index->GetConstraintType(), l_index->GetColumnIds(),
	                          l_index->table_io_manager, l_index->unbound_expressions, storage.db,
	                          l_index->Cast<ART>().allocators);
	if (art->Build(keys, row_ids, row_count) != ARTConflictType::NO_CONFLICT) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	// Merge the ART into the local ART.
	if (!l_index->MergeIndexes(*art)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
}

void ARTBuildSinkDeletes(ARTBuildLocalState &lstate) {
	if (lstate.deleted_keys.empty()) {
		return;
	}
	D_ASSERT(lstate.delete_index);
	lstate.has_delete_index = true;
	auto &delete_index = lstate.delete_index->Cast<ART>();
	auto error = delete_index.InsertKeys(lstate.arena_allocator, lstate.deleted_keys, lstate.deleted_row_ids,
	                                     lstate.deleted_keys.size(), DeleteIndexInfo(),
	                                     IndexAppendMode::INSERT_DUPLICATES, nullptr);
	if (error.HasError()) {
		error.Throw();
	}
}

void ARTBuildSink(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk) {
	auto &bind_data = input.bind_data->Cast<ARTBuildBindData>();
	auto &lstate = input.local_state.Cast<ARTBuildLocalState>();

	lstate.arena_allocator.Reset();

	lstate.local_index->Cast<ART>().GenerateKeyVectors(lstate.arena_allocator, key_chunk, row_chunk.data[0],
	                                                   lstate.keys, lstate.row_ids);

	auto keys = &lstate.keys;
	auto row_ids = &lstate.row_ids;
	auto row_count = key_chunk.size();
	if (bind_data.has_deletes) {
		lstate.visible_keys.clear();
		lstate.visible_row_ids.clear();
		lstate.deleted_keys.clear();
		lstate.deleted_row_ids.clear();

		row_chunk.data[0].Flatten();
		auto row_id_data = FlatVector::GetData<row_t>(row_chunk.data[0]);
		auto &storage = input.table.GetStorage();
		for (idx_t i = 0; i < row_count; i++) {
			if (row_id_data[i] < MAX_ROW_ID && !storage.CanFetch(lstate.transaction, row_id_data[i])) {
				lstate.deleted_keys.push_back(lstate.keys[i]);
				lstate.deleted_row_ids.push_back(lstate.row_ids[i]);
			} else {
				lstate.visible_keys.push_back(lstate.keys[i]);
				lstate.visible_row_ids.push_back(lstate.row_ids[i]);
			}
		}
		ARTBuildSinkDeletes(lstate);
		keys = &lstate.visible_keys;
		row_ids = &lstate.visible_row_ids;
		row_count = keys->size();
	}
	if (row_count == 0) {
		return;
	}
	if (bind_data.sorted) {
		return ARTBuildSinkSorted(input, key_chunk, row_chunk, *keys, *row_ids, row_count);
	}
	return ARTBuildSinkUnsorted(input, key_chunk, row_chunk, *keys, *row_ids, row_count);
}

//----------------------------------------------------------------------------------------------------------------------
// Combine
//----------------------------------------------------------------------------------------------------------------------
void ARTBuildCombine(IndexBuildCombineInput &input) {
	auto &gstate = input.global_state.Cast<ARTBuildGlobalState>();
	auto &lstate = input.local_state.Cast<ARTBuildLocalState>();

	if (!gstate.global_index->MergeIndexes(*lstate.local_index)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
	if (lstate.has_delete_index) {
		D_ASSERT(gstate.delete_index);
		gstate.has_delete_index = true;
		auto error =
		    gstate.delete_index->Cast<ART>().InsertMerge(*lstate.delete_index, IndexAppendMode::INSERT_DUPLICATES);
		if (error.HasError()) {
			error.Throw();
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------------------------------------------------
unique_ptr<BoundIndex> ARTBuildFinalize(IndexBuildFinalizeInput &input) {
	auto &gstate = input.global_state.Cast<ARTBuildGlobalState>();
	if (gstate.has_delete_index) {
		auto error =
		    gstate.global_index->Cast<ART>().InsertMerge(*gstate.delete_index, IndexAppendMode::INSERT_DUPLICATES);
		if (error.HasError()) {
			error.Throw();
		}
	}
	return std::move(gstate.global_index);
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// ART::GetIndexType
//----------------------------------------------------------------------------------------------------------------------
IndexType ART::GetARTIndexType() {
	IndexType art_index_type;
	art_index_type.name = ART::TYPE_NAME;
	art_index_type.create_instance = ART::Create;
	art_index_type.build_bind = ARTBuildBind;
	art_index_type.build_sort = ARTBuildSort;
	art_index_type.build_global_init = ARTBuildGlobalInit;
	art_index_type.build_local_init = ARTBuildLocalInit;
	art_index_type.build_sink = ARTBuildSink;
	art_index_type.build_combine = ARTBuildCombine;
	art_index_type.build_finalize = ARTBuildFinalize;
	return art_index_type;
}

} // namespace duckdb
