#include "duckdb/execution/index/DUMMY/DUMMY.hpp"
#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/index/DUMMY/DUMMY_key.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/execution/index/DUMMY/DUMMY_operator.hpp"

namespace duckdb {
namespace {

//----------------------------------------------------------------------------------------------------------------------
// Bind
//----------------------------------------------------------------------------------------------------------------------
class DUMMYBuildBindData : public IndexBuildBindData {
public:
	bool sorted = false;
};

unique_ptr<IndexBuildBindData> DUMMYBuildBind(IndexBuildBindInput &input) {
	auto bind_data = make_uniq<DUMMYBuildBindData>();

	// TODO: Verify that the the DUMMY is applicable for the given columns and types.
	bind_data->sorted = true;
	if (input.expressions.size() > 1) {
		bind_data->sorted = false;
	} else if (input.expressions[0]->return_type.InternalType() == PhysicalType::VARCHAR) {
		bind_data->sorted = false;
	}

	return std::move(bind_data);
}

bool DUMMYBuildSort(IndexBuildSortInput &input) {
	auto &bind_data = input.bind_data->Cast<DUMMYBuildBindData>();
	return bind_data.sorted;
}

//----------------------------------------------------------------------------------------------------------------------
// Global State
//----------------------------------------------------------------------------------------------------------------------
class DUMMYBuildGlobalState : public IndexBuildGlobalState {
public:
	unique_ptr<BoundIndex> global_index;
};

unique_ptr<IndexBuildGlobalState> DUMMYBuildGlobalInit(IndexBuildInitGlobalStateInput &input) {
	auto state = make_uniq<DUMMYBuildGlobalState>();

	auto &storage = input.table.GetStorage();
	state->global_index = make_uniq<DUMMY>(input.info.index_name, input.info.constraint_type, input.storage_ids,
	                                       TableIOManager::Get(storage), input.expressions, storage.db);

	return std::move(state);
}

//----------------------------------------------------------------------------------------------------------------------
// Local State
//----------------------------------------------------------------------------------------------------------------------
class DUMMYBuildLocalState : public IndexBuildLocalState {
public:
	unique_ptr<BoundIndex> local_index;
	ArenaAllocator arena_allocator;

	unsafe_vector<DUMMYKey> keys;
	unsafe_vector<DUMMYKey> row_ids;

	explicit DUMMYBuildLocalState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};
};

unique_ptr<IndexBuildLocalState> DUMMYBuildLocalInit(IndexBuildInitLocalStateInput &input) {
	// Create the local sink state and add the local index.
	auto state = make_uniq<DUMMYBuildLocalState>(input.context);

	auto &storage = input.table.GetStorage();
	state->local_index = make_uniq<DUMMY>(input.info.index_name, input.info.constraint_type, input.storage_ids,
	                                      TableIOManager::Get(storage), input.expressions, storage.db);

	// Initialize the local sink state.
	state->keys.resize(STANDARD_VECTOR_SIZE);
	state->row_ids.resize(STANDARD_VECTOR_SIZE);

	return std::move(state);
}

//----------------------------------------------------------------------------------------------------------------------
// Sink
//----------------------------------------------------------------------------------------------------------------------
void DUMMYBuildSinkUnsorted(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk) {
	auto &l_state = input.local_state.Cast<DUMMYBuildLocalState>();
	auto row_count = key_chunk.size();
	auto &DUMMY = l_state.local_index->Cast<DUMMY>();

	// Insert each key and its corresponding row ID.
	for (idx_t i = 0; i < row_count; i++) {
		auto status = DUMMY.tree.GetGateStatus();
		auto conflict_type =
		    DUMMYOperator::Insert(l_state.arena_allocator, DUMMY, DUMMY.tree, l_state.keys[i], 0, l_state.row_ids[i],
		                          status, DeleteIndexInfo(), IndexAppendMode::DEFAULT);
		D_ASSERT(conflict_type != DUMMYConflictType::TRANSACTION);
		if (conflict_type == DUMMYConflictType::CONSTRAINT) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}
	}
}

void DUMMYBuildSinkSorted(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk) {
	auto &l_state = input.local_state.Cast<DUMMYBuildLocalState>();
	auto &storage = input.table.GetStorage();
	auto &l_index = l_state.local_index;

	// Construct an DUMMY for this chunk.
	auto DUMMY = make_uniq<DUMMY>(input.info.index_name, l_index->GetConstraintType(), l_index->GetColumnIds(),
	                              l_index->table_io_manager, l_index->unbound_expressions, storage.db,
	                              l_index->Cast<DUMMY>().allocators);
	if (DUMMY->Build(l_state.keys, l_state.row_ids, key_chunk.size()) != DUMMYConflictType::NO_CONFLICT) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}

	// Merge the DUMMY into the local DUMMY.
	if (!l_index->MergeIndexes(*DUMMY)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
}

void DUMMYBuildSink(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk) {
	auto &bind_data = input.bind_data->Cast<DUMMYBuildBindData>();
	auto &lstate = input.local_state.Cast<DUMMYBuildLocalState>();

	lstate.arena_allocator.Reset();

	lstate.local_index->Cast<DUMMY>().GenerateKeyVectors(lstate.arena_allocator, key_chunk, row_chunk.data[0],
	                                                     lstate.keys, lstate.row_ids);

	if (bind_data.sorted) {
		return DUMMYBuildSinkSorted(input, key_chunk, row_chunk);
	}
	return DUMMYBuildSinkUnsorted(input, key_chunk, row_chunk);
}

//----------------------------------------------------------------------------------------------------------------------
// Combine
//----------------------------------------------------------------------------------------------------------------------
void DUMMYBuildCombine(IndexBuildCombineInput &input) {
	auto &gstate = input.global_state.Cast<DUMMYBuildGlobalState>();
	auto &lstate = input.local_state.Cast<DUMMYBuildLocalState>();

	if (!gstate.global_index->MergeIndexes(*lstate.local_index)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------------------------------------------------
unique_ptr<BoundIndex> DUMMYBuildFinalize(IndexBuildFinalizeInput &input) {
	auto &gstate = input.global_state.Cast<DUMMYBuildGlobalState>();
	return std::move(gstate.global_index);
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// DUMMY::GetIndexType
//----------------------------------------------------------------------------------------------------------------------
IndexType DUMMY::GetIndexType() {
	IndexType DUMMY_index_type;
	DUMMY_index_type.name = DUMMY::TYPE_NAME;
	DUMMY_index_type.create_instance = DUMMY::Create;
	DUMMY_index_type.build_bind = DUMMYBuildBind;
	DUMMY_index_type.build_sort = DUMMYBuildSort;
	DUMMY_index_type.build_global_init = DUMMYBuildGlobalInit;
	DUMMY_index_type.build_local_init = DUMMYBuildLocalInit;
	DUMMY_index_type.build_sink = DUMMYBuildSink;
	DUMMY_index_type.build_combine = DUMMYBuildCombine;
	DUMMY_index_type.build_finalize = DUMMYBuildFinalize;
	return DUMMY_index_type;
}

} // namespace duckdb
