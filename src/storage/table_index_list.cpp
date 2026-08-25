#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

IndexEntry::IndexEntry(unique_ptr<Index> index_p) : owned_index(std::move(index_p)) {
	if (owned_index->IsBound()) {
		bind_state = IndexBindState::BOUND;
	} else {
		bind_state = IndexBindState::UNBOUND;
	}
}

void IndexEntry::Append(DataChunk &chunk, Vector &row_ids) {
	auto entry_lock = lock.GetExclusiveLock();
	if (!owned_index->IsBound()) {
		auto &unbound_index = owned_index->Cast<UnboundIndex>();
		unbound_index.BufferChunk(chunk, row_ids, BufferedIndexReplay::INSERT_ENTRY);
		return;
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();
	bound_index.Append(chunk, row_ids);
}

ErrorData IndexEntry::Append(DataChunk &chunk, Vector &row_ids, const shared_ptr<IndexEntry> &delete_entry,
                             IndexAppendMode append_mode, optional_idx active_checkpoint) {
	auto entry_lock = lock.GetExclusiveLock();
	if (!owned_index->IsBound()) {
		auto &unbound_index = owned_index->Cast<UnboundIndex>();
		unbound_index.BufferChunk(chunk, row_ids, BufferedIndexReplay::INSERT_ENTRY);
		return ErrorData();
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();

	unique_ptr<StorageLockKey> delete_lock;
	optional_ptr<const BoundIndex> delete_index;
	if (bound_index.IsUnique() && delete_entry) {
		delete_lock = delete_entry->lock.GetSharedLock();
		D_ASSERT(delete_entry->owned_index->IsBound());
		delete_index = delete_entry->owned_index->Cast<BoundIndex>();
	}

	bool lookup_main_index = false;
	optional_ptr<BoundIndex> append_index;
	if (bound_index.SupportsDeltaIndexes() && deltas.ShouldUse(active_checkpoint)) {
		append_index = deltas.GetOrCreate(bound_index, IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT);
		if (bound_index.IsUnique()) {
			lookup_main_index = true;
		}
	}

	ErrorData error;
	try {
		if (lookup_main_index) {
			IndexAppendInfo lookup_append_info;
			if (delete_index) {
				delete_index->AddToDeleteIndexes(lookup_append_info);
			}
			if (auto delta = deltas.Find(IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT)) {
				delta->AddToDeleteIndexes(lookup_append_info);
			}
			bound_index.VerifyAppend(chunk, lookup_append_info, optional_ptr<ConflictManager>());
		}

		IndexAppendInfo index_append_info(append_mode, nullptr);
		if (delete_index) {
			delete_index->AddToDeleteIndexes(index_append_info);
		}
		if (append_index) {
			error = append_index->Append(chunk, row_ids, index_append_info);
		} else {
			error = bound_index.Append(chunk, row_ids, index_append_info);
		}
	} catch (std::exception &ex) {
		error = ErrorData(ex);
	}
	return error;
}

void IndexEntry::RevertAppend(DataChunk &chunk, Vector &row_ids) {
	auto entry_lock = lock.GetExclusiveLock();
	if (auto delta = deltas.Find(IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT)) {
		delta->Delete(chunk, row_ids);
		return;
	}
	if (owned_index->IsBound()) {
		owned_index->Cast<BoundIndex>().Delete(chunk, row_ids);
	}
}

void IndexEntry::RevertIndexAppend(DataChunk &chunk, Vector &row_ids) {
	auto entry_lock = lock.GetExclusiveLock();
	owned_index->Cast<BoundIndex>().Delete(chunk, row_ids);
}

void IndexEntry::InitializeLocalIndexes(TableIndexList &delete_indexes, TableIndexList &append_indexes) const {
	auto entry_lock = lock.GetSharedLock();
	if (owned_index->GetConstraintType() == IndexConstraintType::NONE || !owned_index->IsBound()) {
		return;
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();
	if (!bound_index.SupportsDeltaIndexes()) {
		return;
	}

	auto constraint_type = bound_index.GetConstraintType();
	delete_indexes.AddIndex(bound_index.CreateEmptyCopy(constraint_type));
	append_indexes.AddIndex(bound_index.CreateEmptyCopy(constraint_type));
}

void IndexEntry::AppendToDeleteIndexes(DataChunk &chunk, Vector &row_ids) {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(owned_index->IsBound());
	if (!owned_index->IsUnique()) {
		return;
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();
	IndexAppendInfo index_append_info(IndexAppendMode::IGNORE_DUPLICATES, nullptr);
	auto result = bound_index.Append(chunk, row_ids, index_append_info);
	if (result.HasError()) {
		throw InternalException("unexpected constraint violation on delete ART: ", result.Message());
	}
}

static void AppendIndexEntries(BoundIndex &index, DataChunk &chunk, Vector &row_ids) {
	IndexAppendInfo append_info;
	const auto error = index.Append(chunk, row_ids, append_info);
	if (error.HasError()) {
		throw InternalException("Failed to append to %s: %s", index.GetIndexName(), error.Message());
	}
}

static bool TryDeleteAll(BoundIndex &index, DataChunk &chunk, Vector &row_ids) {
	const auto delete_count = index.TryDelete(chunk, row_ids, nullptr, nullptr);
	if (delete_count == 0) {
		return false;
	}
	if (delete_count != chunk.size()) {
		// This should not happen: RemoveFromIndexes works on a per-row-group basis, and appends during a checkpoint
		// always use new row groups, so the two groups of data should be separate.
		throw InternalException("IndexEntry::RemoveFromIndex - partially deleted from the checkpoint delta");
	}
	return true;
}

static void ApplyIndexRemoval(BoundIndex &index, IndexDeltas &deltas, DataChunk &chunk, Vector &row_ids,
                              const IndexRemovalType removal_type) {
	const auto supports_delta_indexes = index.SupportsDeltaIndexes();
	// Not all indexes require delta indexes - if an index does not require this we skip creating and appending to
	// "deleted_rows_in_use".
	switch (removal_type) {
	case IndexRemovalType::MAIN_INDEX_ONLY:
		// Directly remove from the main index without appending to delta indexes.
		index.Delete(chunk, row_ids);
		break;
	case IndexRemovalType::REVERT_MAIN_INDEX_ONLY:
		// Revert main index only append - just add back to the index.
		AppendIndexEntries(index, chunk, row_ids);
		break;
	case IndexRemovalType::MAIN_INDEX:
		// Regular removal from the main index - add rows to the delta index if required.
		if (supports_delta_indexes) {
			auto &deleted_rows = deltas.GetOrCreate(index, IndexDeltaType::DELETED_ROWS_IN_USE);
			AppendIndexEntries(deleted_rows, chunk, row_ids);
		}
		index.Delete(chunk, row_ids);
		break;
	case IndexRemovalType::REVERT_MAIN_INDEX:
		// Revert regular append to the main index - remove from deleted_rows_in_use if we appended there before.
		AppendIndexEntries(index, chunk, row_ids);
		if (supports_delta_indexes) {
			if (auto delta = deltas.Find(IndexDeltaType::DELETED_ROWS_IN_USE)) {
				delta->Delete(chunk, row_ids);
			}
		}
		break;
	case IndexRemovalType::DELETED_ROWS_IN_USE:
		// Remove from the removal index if we appended any rows.
		if (supports_delta_indexes) {
			if (auto delta = deltas.Find(IndexDeltaType::DELETED_ROWS_IN_USE)) {
				delta->Delete(chunk, row_ids);
			}
		}
		break;
	default:
		throw InternalException("Unsupported IndexRemovalType");
	}
}

static void ApplyIndexRemovalDuringCheckpoint(BoundIndex &index, IndexDeltas &deltas, DataChunk &chunk, Vector &row_ids,
                                              const IndexRemovalType removal_type) {
	D_ASSERT(removal_type != IndexRemovalType::DELETED_ROWS_IN_USE);

	switch (removal_type) {
	case IndexRemovalType::MAIN_INDEX_ONLY:
	case IndexRemovalType::MAIN_INDEX: {
		// Removing from the main index cannot happen directly due to the concurrent checkpoint; add the removal to a
		// delta index.
		auto &removed_data = deltas.GetOrCreate(index, IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT);
		auto added_data = deltas.Find(IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT);

		// If we have also added data during this checkpoint, we might need to remove from there instead.
		// We FIRST try to remove from "added_data_during_checkpoint"; any rows that are not there are added to
		// "removed_data_during_checkpoint".
		if (!added_data || !TryDeleteAll(*added_data, chunk, row_ids)) {
			AppendIndexEntries(removed_data, chunk, row_ids);
		}
		if (removal_type == IndexRemovalType::MAIN_INDEX) {
			// MAIN_INDEX also needs to retain the rows in deleted_rows_in_use.
			auto &deleted_rows = deltas.GetOrCreate(index, IndexDeltaType::DELETED_ROWS_IN_USE);
			AppendIndexEntries(deleted_rows, chunk, row_ids);
		}
		break;
	}
	case IndexRemovalType::REVERT_MAIN_INDEX_ONLY:
	case IndexRemovalType::REVERT_MAIN_INDEX: {
		auto &removed_data = deltas.GetOrCreate(index, IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT);
		// Revert adding to the main index.
		// We have added data during this checkpoint as well, so the removal might have EITHER:
		// (1) added data to "removed_data_during_checkpoint" or
		// (2) removed data from "added_data_during_checkpoint".
		// Revert by first trying to remove from "removed_data_during_checkpoint"; any rows that were not removed are
		// re-added to "added_data_during_checkpoint".
		if (auto added_data = deltas.Find(IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT)) {
			if (!TryDeleteAll(removed_data, chunk, row_ids)) {
				AppendIndexEntries(*added_data, chunk, row_ids);
			}
		} else {
			removed_data.Delete(chunk, row_ids);
		}
		if (removal_type == IndexRemovalType::REVERT_MAIN_INDEX) {
			// We also need to remove from "deleted_rows_in_use".
			if (auto delta = deltas.Find(IndexDeltaType::DELETED_ROWS_IN_USE)) {
				delta->Delete(chunk, row_ids);
			}
		}
		break;
	}
	default:
		throw InternalException("Unsupported IndexRemovalType");
	}
}

void IndexEntry::RemoveFromIndex(DataChunk &chunk, Vector &row_ids, const IndexRemovalType removal_type,
                                 const optional_idx active_checkpoint) {
	auto entry_lock = lock.GetExclusiveLock();
	if (!owned_index->IsBound()) {
		// Buffer the delete: chunk is in table layout with all indexed columns populated.
		owned_index->Cast<UnboundIndex>().BufferChunk(chunk, row_ids, BufferedIndexReplay::DEL_ENTRY);
		return;
	}

	auto &bound_index = owned_index->Cast<BoundIndex>();
	// Check which indexes we should append to or remove from. This method might also involve appending to indexes:
	// delta indexes must be filled with data we are removing, or we may be reverting a previous removal.
	// Not all indexes require delta indexes, so those skip "deleted_rows_in_use" bookkeeping.
	if (removal_type == IndexRemovalType::DELETED_ROWS_IN_USE) {
		// Cleanup always removes directly from "deleted_rows_in_use", even during a checkpoint.
		ApplyIndexRemoval(bound_index, deltas, chunk, row_ids, removal_type);
	} else if (bound_index.SupportsDeltaIndexes() && deltas.ShouldUse(active_checkpoint)) {
		// During a checkpoint, route changes through the checkpoint deltas instead of the main index.
		ApplyIndexRemovalDuringCheckpoint(bound_index, deltas, chunk, row_ids, removal_type);
	} else {
		ApplyIndexRemoval(bound_index, deltas, chunk, row_ids, removal_type);
	}
}

bool IndexEntry::IsUnique() const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->IsUnique();
}

bool IndexEntry::IsART() const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->GetIndexType() == ART::TYPE_NAME;
}

bool IndexEntry::ConflictTargetMatches(const ConflictInfo &conflict_info) const {
	auto entry_lock = lock.GetSharedLock();
	return conflict_info.ConflictTargetMatches(owned_index->IsUnique(), owned_index->GetColumnIdSet());
}

bool IndexEntry::IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const ForeignKeyType fk_type) const {
	auto entry_lock = lock.GetSharedLock();
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !owned_index->IsUnique() : !owned_index->IsForeign()) {
		return false;
	}
	const auto &column_ids = owned_index->GetColumnIds();
	if (fk_keys.size() != column_ids.size()) {
		return false;
	}

	for (const auto &fk_key : fk_keys) {
		bool found = false;
		for (const auto index_key : column_ids) {
			if (fk_key.index == index_key) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

bool IndexEntry::NameEquals(const Identifier &name) const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->GetIndexName() == name;
}

Identifier IndexEntry::GetName() const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->GetIndexName();
}

void IndexEntry::VerifyAppend(const shared_ptr<IndexEntry> &delete_entry, DataChunk &chunk,
                              optional_ptr<ConflictManager> manager) {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(owned_index->IsBound());
	auto &bound_index = owned_index->Cast<BoundIndex>();

	IndexAppendInfo index_append_info;
	unique_ptr<StorageLockKey> delete_lock;
	if (delete_entry) {
		delete_lock = delete_entry->lock.GetSharedLock();
		D_ASSERT(delete_entry->owned_index->IsBound());
		delete_entry->owned_index->Cast<BoundIndex>().AddToDeleteIndexes(index_append_info);
	}
	if (!manager) {
		if (auto delta = deltas.Find(IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT)) {
			delta->AddToDeleteIndexes(index_append_info);
		}
	}
	bound_index.VerifyAppend(chunk, index_append_info, manager);
}

void IndexEntry::VerifyForeignKey(const shared_ptr<IndexEntry> &delete_entry, DataChunk &chunk,
                                  ConflictManager &conflict_manager) {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(owned_index->IsBound());
	auto &bound_index = owned_index->Cast<BoundIndex>();

	IndexAppendInfo index_append_info;
	unique_ptr<StorageLockKey> delete_lock;
	if (delete_entry) {
		delete_lock = delete_entry->lock.GetSharedLock();
		D_ASSERT(delete_entry->owned_index->IsBound());
		delete_entry->owned_index->Cast<BoundIndex>().AddToDeleteIndexes(index_append_info);
	}
	if (auto delta = deltas.Find(IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT)) {
		delta->AddToDeleteIndexes(index_append_info);
	}

	bound_index.VerifyConstraint(chunk, index_append_info, conflict_manager);
	if (auto delta = deltas.Find(IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT)) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_during_checkpoint_info;
		delta->VerifyConstraint(chunk, added_during_checkpoint_info, conflict_manager);
	}
}

string IndexEntry::GetConstraintViolationMessage(const VerifyExistenceType verify_type, const idx_t failed_index,
                                                 DataChunk &input) const {
	auto entry_lock = lock.GetSharedLock();
	D_ASSERT(owned_index->IsBound());
	return owned_index->Cast<BoundIndex>().GetConstraintViolationMessage(verify_type, failed_index, input);
}

void IndexEntry::VerifyUpdate(const vector<PhysicalIndex> &column_ids) const {
#ifdef DEBUG
	auto entry_lock = lock.GetSharedLock();
	D_ASSERT(owned_index->IsBound());
	const auto &bound_index = owned_index->Cast<BoundIndex>();
	D_ASSERT(!bound_index.IndexIsUpdated(column_ids));
#endif
}

void IndexEntry::Vacuum() {
	auto entry_lock = lock.GetExclusiveLock();
	if (owned_index->IsBound()) {
		owned_index->Cast<BoundIndex>().Vacuum();
	}
}

void IndexEntry::Rebuild(const IndexRebuildScan &scan) {
	auto entry_lock = lock.GetExclusiveLock();
	if (!owned_index->IsBound()) {
		throw InternalException("RebuildIndexes expects all indexes to be bound during checkpoint");
	}

	auto &bound_index = owned_index->Cast<BoundIndex>();
	bound_index.ResetStorage();

	IndexRebuildAppend append = [&](DataChunk &chunk, Vector &row_ids) {
		auto error = bound_index.Append(chunk, row_ids);
		if (error.HasError()) {
			throw InternalException("Failed to rebuild index '%s' after vacuum: %s", bound_index.GetIndexName(),
			                        error.Message());
		}
	};
	scan(bound_index.GetColumnIds(), append);
	bound_index.Verify();
}

void IndexEntry::VerifyBuffers() {
	auto entry_lock = lock.GetExclusiveLock();
	if (auto delta = deltas.Find(IndexDeltaType::DELETED_ROWS_IN_USE)) {
		delta->VerifyBuffers();
	}
	if (owned_index->IsBound()) {
		owned_index->Cast<BoundIndex>().VerifyBuffers();
	}
}

IndexInfo IndexEntry::GetStorageInfo() const {
	auto entry_lock = lock.GetSharedLock();
	IndexInfo result;
	result.is_primary = owned_index->IsPrimary();
	result.is_unique = owned_index->IsUnique() || result.is_primary;
	result.is_foreign = owned_index->IsForeign();
	result.column_set = owned_index->GetColumnIdSet();
	return result;
}

idx_t IndexEntry::GetInMemorySize() const {
	auto entry_lock = lock.GetSharedLock();
	if (!owned_index->IsBound()) {
		return 0;
	}
	return owned_index->Cast<BoundIndex>().GetInMemorySize();
}

IndexStorageInfo IndexEntry::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) {
	auto entry_lock = lock.GetExclusiveLock();
	if (owned_index->IsBound()) {
		return owned_index->Cast<BoundIndex>().SerializeToDisk(context, options);
	}
	return owned_index->Cast<UnboundIndex>().CopyStorageInfo();
}

IndexStorageInfo IndexEntry::SerializeToWAL(const case_insensitive_map_t<Value> &options) {
	auto entry_lock = lock.GetExclusiveLock();
	// We never write an unbound index to the WAL.
	D_ASSERT(owned_index->IsBound());
	return owned_index->Cast<BoundIndex>().SerializeToWAL(options);
}

void IndexEntry::MergeCheckpointDeltas(const transaction_t checkpoint_id) {
	auto entry_lock = lock.GetExclusiveLock();
	// Merge any data appended to the index while the checkpoint was running.
	if (!owned_index->IsBound()) {
		return;
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();
	auto error = deltas.MergeCheckpointDeltas(bound_index);
	if (error.HasError()) {
		throw InternalException("Failed to merge checkpoint delta - this signifies a bug or broken index: %s",
		                        error.Message());
	}
	deltas.MarkWritten(checkpoint_id);
}

const unique_ptr<BoundIndex> &IndexDeltas::GetPointer(const IndexDeltaType type) const {
	switch (type) {
	case IndexDeltaType::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use;
	case IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT:
		return checkpoint.added_data;
	case IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT:
		return checkpoint.removed_data;
	}
	throw InternalException("Unsupported index delta type");
}

unique_ptr<BoundIndex> &IndexDeltas::GetPointer(const IndexDeltaType type) {
	switch (type) {
	case IndexDeltaType::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use;
	case IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT:
		return checkpoint.added_data;
	case IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT:
		return checkpoint.removed_data;
	}
	throw InternalException("Unsupported index delta type");
}

optional_ptr<const BoundIndex> IndexDeltas::Find(const IndexDeltaType type) const {
	return GetPointer(type).get();
}

optional_ptr<BoundIndex> IndexDeltas::Find(const IndexDeltaType type) {
	return GetPointer(type).get();
}

BoundIndex &IndexDeltas::GetOrCreate(BoundIndex &index, const IndexDeltaType type) {
	auto &delta = GetPointer(type);
	if (!delta) {
		D_ASSERT(index.SupportsDeltaIndexes());
		auto constraint_type = index.GetConstraintType();
		if (type == IndexDeltaType::DELETED_ROWS_IN_USE) {
			// deleted_rows_in_use allows duplicates regardless of whether the main index is unique
			constraint_type = IndexConstraintType::NONE;
		}
		delta = index.CreateEmptyCopy(constraint_type);
		D_ASSERT(delta);
	}
	return *delta;
}

bool IndexDeltas::ShouldUse(const optional_idx active_checkpoint) const {
	if (!active_checkpoint.IsValid()) {
		return false;
	}
	if (!checkpoint.last_written_checkpoint.IsValid()) {
		return true;
	}
	return active_checkpoint.GetIndex() != checkpoint.last_written_checkpoint.GetIndex();
}

ErrorData IndexDeltas::MergeCheckpointDeltas(BoundIndex &index) {
	for (const auto type :
	     {IndexDeltaType::REMOVED_DATA_DURING_CHECKPOINT, IndexDeltaType::ADDED_DATA_DURING_CHECKPOINT}) {
		auto &delta = GetPointer(type);
		if (!delta) {
			continue;
		}
		auto error = index.MergeCheckpointDelta(type, *delta);
		if (error.HasError()) {
			return error;
		}
		delta.reset();
	}
	return ErrorData();
}

void IndexDeltas::MarkWritten(const transaction_t checkpoint_id) {
	checkpoint.last_written_checkpoint = checkpoint_id;
}

template <class T>
TableIndexIterationHelper<T>::TableIndexIterationHelper(const TableIndexList &index_list)
    : lock(index_list.index_entries_lock), index_entries(index_list.index_entries) {
}

template <class T>
TableIndexIterationHelper<T>::TableIndexIterator::TableIndexIterator(
    optional_ptr<const vector<shared_ptr<IndexEntry>>> index_entries_p)
    : index_entries(index_entries_p) {
	if (index_entries) {
		if (index_entries->empty()) {
			index_entries = nullptr;
		} else {
			index = 0;
		}
	}
}

template <class T>
typename TableIndexIterationHelper<T>::TableIndexIterator &
TableIndexIterationHelper<T>::TableIndexIterator::operator++() {
	if (index_entries) {
		auto next_index = index.GetIndex() + 1;
		if (next_index >= index_entries->size()) {
			// reached the end
			index = optional_idx();
			index_entries = nullptr;
		} else {
			// next index
			index = next_index;
		}
	}
	return *this;
}

template <class T>
bool TableIndexIterationHelper<T>::TableIndexIterator::operator!=(const TableIndexIterator &other) const {
	return index != other.index || index_entries != other.index_entries;
}

TableIndexIterationHelper<IndexHandle<Index>> TableIndexList::IndexHandles() const {
	return TableIndexIterationHelper<IndexHandle<Index>>(*this);
}

TableIndexIterationHelper<shared_ptr<IndexEntry>> TableIndexList::IndexEntries() const {
	return TableIndexIterationHelper<shared_ptr<IndexEntry>>(*this);
}

vector<shared_ptr<IndexEntry>> TableIndexList::GetEntries() const {
	annotated_lock_guard lock(index_entries_lock);
	return index_entries;
}

template <>
IndexHandle<Index> TableIndexIterationHelper<IndexHandle<Index>>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex())->GetHandle();
}

template <>
shared_ptr<IndexEntry> TableIndexIterationHelper<shared_ptr<IndexEntry>>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex());
}

template class TableIndexIterationHelper<IndexHandle<Index>>;
template class TableIndexIterationHelper<shared_ptr<IndexEntry>>;

void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	annotated_lock_guard lock(index_entries_lock);
	auto index_entry = make_shared_ptr<IndexEntry>(std::move(index));
	index_entries.push_back(std::move(index_entry));
	const auto index_handle = index_entries.back()->GetHandle();
	if (!index_handle->IsBound()) {
		unbound_count++;
	}
}

void TableIndexList::InitializeLocalIndexes(TableIndexList &delete_indexes, TableIndexList &append_indexes) const {
	D_ASSERT(this != &delete_indexes);
	D_ASSERT(this != &append_indexes);
	D_ASSERT(&delete_indexes != &append_indexes);
	D_ASSERT(delete_indexes.Empty());
	D_ASSERT(append_indexes.Empty());

	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->InitializeLocalIndexes(delete_indexes, append_indexes);
	}
}

void TableIndexList::Append(DataChunk &chunk, Vector &row_ids) {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->Append(chunk, row_ids);
	}
}

ErrorData TableIndexList::Append(optional_ptr<TableIndexList> delete_indexes, DataChunk &chunk, row_t row_start,
                                 IndexAppendMode append_mode, optional_idx active_checkpoint) {
	Vector row_ids(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_ids, chunk.size(), row_start, 1);

	annotated_lock_guard lock(index_entries_lock);
	vector<shared_ptr<IndexEntry>> already_appended;

	ErrorData error;
	for (const auto &entry : index_entries) {
		shared_ptr<IndexEntry> delete_entry;
		if (delete_indexes && entry->IsUnique()) {
			delete_entry = delete_indexes->FindEntry(entry->GetName());
		}
		error = entry->Append(chunk, row_ids, delete_entry, append_mode, active_checkpoint);
		if (error.HasError()) {
			break;
		}
		already_appended.push_back(entry);
	}

	if (error.HasError()) {
		for (auto &entry : already_appended) {
			entry->RevertAppend(chunk, row_ids);
		}
	}
	return error;
}

void TableIndexList::RevertAppend(DataChunk &chunk, Vector &row_ids) {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->RevertAppend(chunk, row_ids);
	}
}

void TableIndexList::RevertIndexAppend(DataChunk &chunk, row_t row_start) {
	Vector row_ids(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_ids, chunk.size(), row_start, 1);

	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->RevertIndexAppend(chunk, row_ids);
	}
}

void TableIndexList::AppendToDeleteIndexes(DataChunk &chunk, Vector &row_ids) {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->AppendToDeleteIndexes(chunk, row_ids);
	}
}

void TableIndexList::RemoveFromIndexes(DataChunk &chunk, Vector &row_ids, const IndexRemovalType removal_type,
                                       const optional_idx active_checkpoint) {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->RemoveFromIndex(chunk, row_ids, removal_type, active_checkpoint);
	}
}

void TableIndexList::RemoveIndex(const Identifier &name) {
	annotated_lock_guard lock(index_entries_lock);
	for (idx_t i = 0; i < index_entries.size(); i++) {
		auto index = index_entries[i]->GetMutableHandle();
		if (index->GetIndexName() == name) {
			if (!index->IsBound()) {
				unbound_count--;
			}
			index->ResetStorage();
			index_entries.erase_at(i);
			return;
		}
	}
}

bool TableIndexList::HasUniqueIndexes() const {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		if (entry->IsUnique()) {
			return true;
		}
	}
	return false;
}

void TableIndexList::VerifyUniqueIndexes(optional_ptr<const TableIndexList> delete_indexes, DataChunk &chunk,
                                         optional_ptr<ConflictManager> manager) const {
	annotated_lock_guard lock(index_entries_lock);
	if (!manager) {
		for (const auto &entry : index_entries) {
			if (!entry->IsUnique() || !entry->IsART()) {
				continue;
			}
			auto delete_entry = delete_indexes ? delete_indexes->FindEntry(entry->GetName()) : nullptr;
			entry->VerifyAppend(delete_entry, chunk, nullptr);
		}
		return;
	}

	// Find all indexes matching the conflict target.
	const auto &conflict_info = manager->GetConflictInfo();
	for (const auto &entry : index_entries) {
		if (!entry->IsUnique() || !entry->IsART() || !entry->ConflictTargetMatches(conflict_info)) {
			continue;
		}
		auto index_name = entry->GetName();
		auto delete_entry = delete_indexes ? delete_indexes->FindEntry(index_name) : nullptr;
		manager->AddIndex(entry, index_name, std::move(delete_entry));
	}

	// Verify indexes matching the conflict target.
	manager->SetMode(ConflictManagerMode::SCAN);
	const auto &matching_indexes = manager->MatchingIndexes();
	const auto &matching_delete_indexes = manager->MatchingDeleteIndexes();
	for (idx_t i = 0; i < matching_indexes.size(); i++) {
		matching_indexes[i]->VerifyAppend(matching_delete_indexes[i], chunk, manager);
	}

	// Scan the other indexes and throw if there are any conflicts.
	manager->SetMode(ConflictManagerMode::THROW);
	for (const auto &entry : index_entries) {
		if (!entry->IsUnique() || !entry->IsART() || manager->IndexMatches(entry->GetName())) {
			continue;
		}
		auto delete_entry = delete_indexes ? delete_indexes->FindEntry(entry->GetName()) : nullptr;
		entry->VerifyAppend(delete_entry, chunk, manager);
	}
}

void TableIndexList::Vacuum() {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->Vacuum();
	}
}

void TableIndexList::Rebuild(const IndexRebuildScan &scan) {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->Rebuild(scan);
	}
}

void TableIndexList::VerifyBuffers() const {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->VerifyBuffers();
	}
}

void TableIndexList::VerifyUpdate(const vector<PhysicalIndex> &column_ids) const {
#ifdef DEBUG
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->VerifyUpdate(column_ids);
	}
#else
	(void)column_ids;
#endif
}

vector<IndexInfo> TableIndexList::GetStorageInfo() const {
	annotated_lock_guard lock(index_entries_lock);
	vector<IndexInfo> result;
	result.reserve(index_entries.size());
	for (const auto &entry : index_entries) {
		result.push_back(entry->GetStorageInfo());
	}
	return result;
}

idx_t TableIndexList::GetInMemorySize() const {
	annotated_lock_guard lock(index_entries_lock);
	idx_t result = 0;
	for (const auto &entry : index_entries) {
		result += entry->GetInMemorySize();
	}
	return result;
}

unordered_set<string> TableIndexList::DistinctIndexTypes() const {
	unordered_set<string> result;
	for (const auto index : IndexHandles()) {
		result.insert(index->GetIndexType());
	}
	return result;
}

bool TableIndexList::AllIndexesBoundOfType(const char *index_type) const {
	for (const auto index : IndexHandles()) {
		if (!index->IsBound() || index->GetIndexType() != index_type) {
			return false;
		}
	}
	return true;
}

bool TableIndexList::NameIsUnique(const string &name) const {
	// Only covers PK, FK, and UNIQUE indexes.
	for (const auto index : IndexHandles()) {
		if (index->IsPrimary() || index->IsForeign() || index->IsUnique()) {
			if (index->GetIndexName() == name) {
				return false;
			}
		}
	}
	return true;
}

bool TableIndexList::Contains(const Identifier &name) const {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		if (entry->NameEquals(name)) {
			return true;
		}
	}
	return false;
}

shared_ptr<IndexEntry> TableIndexList::FindEntry(const Identifier &name) const {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		const auto index = entry->GetHandle();
		if (index->GetIndexName() != name) {
			continue;
		}
		if (!index->IsBound()) {
			throw InternalException("TableIndexList::FindEntry cannot return an unbound index");
		}
		return entry;
	}
	return nullptr;
}

void TableIndexList::Bind(ClientContext &context, DataTableInfo &table_info, const char *index_type) {
	{
		// Early-out, if we have no unbound indexes.
		annotated_lock_guard lock(index_entries_lock);
		if (unbound_count == 0) {
			return;
		}
	}

	// Get the table from the catalog, so we can add it to the binder.
	auto &catalog = table_info.GetDB().GetCatalog();
	// the table can live in a nested schema - qualify it with the full schema path
	auto schema_path = table_info.GetSchemaPath();
	schema_path.insert(schema_path.begin(), catalog.GetName());
	auto &table_entry =
	    catalog.GetEntry<TableCatalogEntry>(context, QualifiedName(std::move(schema_path), table_info.GetTableName()));
	auto &table = table_entry.Cast<DuckTableEntry>();

	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.emplace_back(col.Name());
	}

	annotated_unique_lock lock(index_entries_lock);
	// Busy-spin trying to bind all indexes.
	while (true) {
		shared_ptr<IndexEntry> index_entry;
		for (auto &entry : index_entries) {
			auto index = entry->GetHandle();
			if (!index->IsBound() && (index_type == nullptr || index->GetIndexType() == index_type)) {
				index_entry = entry;
				break;
			}
		}
		if (!index_entry) {
			// We bound all indexes. (of this type)
			break;
		}
		if (index_entry->GetBindState() == IndexBindState::BINDING) {
			// Another thread is binding the index.
			// Lock and unlock the index entries so that the other thread can commit its changes.
			lock.unlock();
			lock.lock();
			continue;

		} else if (index_entry->GetBindState() == IndexBindState::UNBOUND) {
			// We are the thread that'll bind the index.
			index_entry->SetBindState(IndexBindState::BINDING);
			lock.unlock();

		} else {
			throw InternalException("index entry bind state cannot be BOUND here");
		}

		// Create a binder to bind this index.
		auto binder = Binder::CreateBinder(context);

		// Add the table to the binder.
		vector<ColumnIndex> dummy_column_ids;
		binder->bind_context.AddBaseTable(TableIndex(0), Identifier(), StringsToIdentifiers(column_names), column_types,
		                                  dummy_column_ids, table);

		// Create an IndexBinder to bind the index
		IndexBinder idx_binder(*binder, context);

		// Apply any outstanding buffered replays and replace the unbound index with a bound index.
		unique_ptr<BoundIndex> bound_idx;
		{
			auto index = index_entry->GetMutableHandle<UnboundIndex>();
			vector<LogicalType> physical_column_types;
			for (auto &col : table.GetColumns().Physical()) {
				physical_column_types.push_back(col.Type());
			}
			bound_idx = index->Bind(idx_binder, physical_column_types);
		}

		// Commit the bound index to the index entry.
		lock.lock();
		auto current_entry = std::find(index_entries.begin(), index_entries.end(), index_entry);
		if (current_entry == index_entries.end()) {
			continue;
		}
		auto index = index_entry->GetMutableHandle();
		index_entry->SetBindState(IndexBindState::BOUND);
		index.ReplaceIndex(std::move(bound_idx));
		unbound_count--;
	}
}

shared_ptr<IndexEntry> TableIndexList::FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys,
                                                           const ForeignKeyType fk_type) {
	annotated_lock_guard<annotated_mutex> lock(index_entries_lock);
	for (auto &entry : index_entries) {
		if (entry->IsForeignKeyIndex(fk_keys, fk_type)) {
			return entry;
		}
	}
	return nullptr;
}

void TableIndexList::VerifyForeignKey(optional_ptr<const TableIndexList> delete_indexes,
                                      const vector<PhysicalIndex> &fk_keys, DataChunk &chunk,
                                      ConflictManager &conflict_manager) {
	const auto fk_type = conflict_manager.GetVerifyExistenceType() == VerifyExistenceType::APPEND_FK
	                         ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE
	                         : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	// Check whether the chunk can be inserted in or deleted from the referenced table storage.
	annotated_lock_guard lock(index_entries_lock);
	shared_ptr<IndexEntry> entry;
	for (const auto &candidate : index_entries) {
		if (candidate->IsForeignKeyIndex(fk_keys, fk_type)) {
			entry = candidate;
			break;
		}
	}
	if (!entry) {
		throw InternalException("TableIndexList::VerifyForeignKey failed to find foreign key index");
	}

	auto delete_entry = delete_indexes ? delete_indexes->FindEntry(entry->GetName()) : nullptr;
	entry->VerifyForeignKey(delete_entry, chunk, conflict_manager);
}

unordered_set<column_t> TableIndexList::GetIndexedColumns() const {
	unordered_set<column_t> column_ids;
	for (auto index : IndexHandles()) {
		for (auto col_id : index->GetColumnIds()) {
			column_ids.insert(col_id);
		}
	}
	return column_ids;
}

vector<unordered_set<column_t>> TableIndexList::GetConflictTargetColumns(const ConflictInfo &conflict_info) const {
	annotated_lock_guard lock(index_entries_lock);
	vector<unordered_set<column_t>> result;
	for (const auto &entry : index_entries) {
		auto index_info = entry->GetStorageInfo();
		if (!index_info.is_unique ||
		    !conflict_info.ConflictTargetMatches(index_info.is_unique, index_info.column_set)) {
			continue;
		}
		D_ASSERT(entry->GetBindState() == IndexBindState::BOUND);
		result.push_back(std::move(index_info.column_set));
	}
	return result;
}

unordered_set<column_t> TableIndexList::GetUniqueIndexColumns() const {
	annotated_lock_guard lock(index_entries_lock);
	unordered_set<column_t> result;
	for (const auto &entry : index_entries) {
		auto index_info = entry->GetStorageInfo();
		if (!index_info.is_unique) {
			continue;
		}
		result.insert(index_info.column_set.begin(), index_info.column_set.end());
	}
	return result;
}

IndexSerializationResult TableIndexList::SerializeToDisk(QueryContext context, const IndexSerializationInfo &info) {
	annotated_lock_guard<annotated_mutex> lock(index_entries_lock);

	IndexSerializationResult result;

	result.owned_infos.reserve(index_entries.size());
	for (const auto &entry : index_entries) {
		auto storage_info = entry->SerializeToDisk(context, info.options);
		D_ASSERT(storage_info.IsValid() && !storage_info.name.empty());
		result.owned_infos.push_back(std::move(storage_info));
		result.ordered_infos.push_back(result.owned_infos.back());
	}

	return result;
}

unique_ptr<IndexStorageInfo> TableIndexList::SerializeToWAL(const Identifier &name,
                                                            const case_insensitive_map_t<Value> &options) {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		if (entry->NameEquals(name)) {
			return make_uniq<IndexStorageInfo>(entry->SerializeToWAL(options));
		}
	}
	return nullptr;
}

void TableIndexList::MergeCheckpointDeltas(const transaction_t checkpoint_id) const {
	annotated_lock_guard lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		entry->MergeCheckpointDeltas(checkpoint_id);
	}
}

void TableIndexList::InitializeIndexChunk(DataChunk &index_chunk, const vector<LogicalType> &table_types,
                                          vector<StorageIndex> &mapped_column_ids, DataTableInfo &data_table_info) {
	// table_chunk contains all table columns.
	// We only reference the index columns in the index chunk.
	auto &index_list = data_table_info.GetIndexes();
	auto indexed_columns = index_list.GetIndexedColumns();

	// Store the mapped_column_ids and index_types in sorted canonical form.
	// First sort mapped_column_ids, then populate index_types according to the sorted order.
	for (auto &col : indexed_columns) {
		mapped_column_ids.emplace_back(col);
	}
	std::sort(mapped_column_ids.begin(), mapped_column_ids.end());

	vector<LogicalType> index_types;
	for (auto &col : mapped_column_ids) {
		index_types.push_back(table_types[col.GetPrimaryIndex()]);
	}

	index_chunk.InitializeEmpty(index_types);
}

void TableIndexList::ReferenceIndexChunk(DataChunk &table_chunk, DataChunk &index_chunk,
                                         vector<StorageIndex> &mapped_column_ids) {
	for (idx_t i = 0; i < mapped_column_ids.size(); i++) {
		auto col_id = mapped_column_ids[i].GetPrimaryIndex();
		index_chunk.data[i].Reference(table_chunk.data[col_id]);
	}
}

} // namespace duckdb
