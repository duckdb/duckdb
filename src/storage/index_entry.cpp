#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/checkpoint/table_index_writer.hpp"

#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"

namespace duckdb {

IndexEntry::IndexEntry(unique_ptr<Index> index_p, idx_t index_oid_p, const ConstraintCheckMode check_mode_p)
    : index_oid(index_oid_p), last_commit(0), owned_index(std::move(index_p)), check_mode(check_mode_p),
      building(false), build_transaction(0) {
	if (owned_index->IsBound()) {
		bind_state = IndexBindState::BOUND;
	} else {
		bind_state = IndexBindState::UNBOUND;
	}
}

IndexEntry::IndexEntry(const BoundIndex &index_template, transaction_t build_commit, transaction_t build_transaction_p)
    : bind_state(IndexBindState::BOUND), index_oid(DConstants::INVALID_INDEX), last_commit(build_commit),
      owned_index(index_template.CreateEmptyCopy(IndexConstraintType::NONE)), check_mode(ConstraintCheckMode::DEFAULT),
      building(true), build_transaction(build_transaction_p) {
	D_ASSERT(owned_index->GetConstraintType() == IndexConstraintType::NONE);
	D_ASSERT(owned_index->Cast<BoundIndex>().SupportsDeltaIndexes());
}

void IndexEntry::Append(DataChunk &chunk, Vector &row_ids) {
	auto entry_lock = lock.GetExclusiveLock();
	if (!owned_index->IsBound()) {
		auto &unbound_index = owned_index->Cast<UnboundIndex>();
		unbound_index.BufferChunk(chunk, row_ids, BufferedIndexReplay::INSERT_ENTRY);
		return;
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();
	auto error = bound_index.Append(chunk, row_ids);
	if (error.HasError()) {
		error.Throw();
	}
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

	if (building) {
		// a building index never fails a commit: an error fails the build instead
		if (build_error.HasError()) {
			return ErrorData();
		}
		build_changed_rows += chunk.size();
		try {
			auto &added_data = deltas.GetOrCreate(bound_index, IndexDeltaType::ADDED_DATA);
			IndexAppendInfo index_append_info(append_mode, nullptr);
			auto error = added_data.Append(chunk, row_ids, index_append_info);
			if (error.HasError()) {
				build_error = std::move(error);
			}
		} catch (std::exception &ex) {
			RecordBuildError(ex);
		}
		return ErrorData();
	}

	bool lookup_main_index = false;
	optional_ptr<BoundIndex> append_index;
	if (bound_index.SupportsDeltaIndexes() && UseDeltas(active_checkpoint)) {
		append_index = deltas.GetOrCreate(bound_index, IndexDeltaType::ADDED_DATA);
		if (bound_index.IsUnique()) {
			lookup_main_index = true;
		}
	}

	try {
		if (lookup_main_index) {
			IndexAppendInfo lookup_append_info;
			if (delete_index) {
				lookup_append_info.delete_indexes.push_back(*delete_index);
			}
			if (auto delta = deltas.Find(IndexDeltaType::REMOVED_DATA)) {
				lookup_append_info.delete_indexes.push_back(*delta);
			}
			bound_index.VerifyAppend(chunk, lookup_append_info, optional_ptr<ConflictManager>());
		}
	} catch (std::exception &ex) {
		// the lookup is read-only: nothing was inserted yet
		return ErrorData(ex);
	}

	IndexAppendInfo index_append_info(append_mode, nullptr);
	if (delete_index) {
		index_append_info.delete_indexes.push_back(*delete_index);
	}
	if (append_index) {
		return append_index->Append(chunk, row_ids, index_append_info);
	}
	return bound_index.Append(chunk, row_ids, index_append_info);
}

void IndexEntry::RecordBuildError(std::exception &ex) {
	ErrorData error(ex);
	if (Exception::InvalidatesDatabase(error.Type())) {
		// the database is already invalidated: don't hide it
		throw;
	}
	build_error = std::move(error);
}

void IndexEntry::RevertAppend(DataChunk &chunk, Vector &row_ids) {
	auto entry_lock = lock.GetExclusiveLock();
	if (building && build_error.HasError()) {
		return;
	}
	if (auto delta = deltas.Find(IndexDeltaType::ADDED_DATA)) {
		delta->Delete(chunk, row_ids);
		return;
	}
	if (owned_index->IsBound()) {
		owned_index->Cast<BoundIndex>().Delete(chunk, row_ids);
	}
}

void IndexEntry::InitializeLocalIndexes(TableIndexList &delete_indexes, TableIndexList &append_indexes) const {
	InitializeLocalIndexesInternal(delete_indexes, &append_indexes);
}

void IndexEntry::InitializeLocalDeleteIndex(TableIndexList &delete_indexes) const {
	InitializeLocalIndexesInternal(delete_indexes, nullptr);
}

void IndexEntry::InitializeLocalIndexesInternal(TableIndexList &delete_indexes,
                                                optional_ptr<TableIndexList> append_indexes) const {
	auto entry_lock = lock.GetSharedLock();
	if (owned_index->GetConstraintType() == IndexConstraintType::NONE || !owned_index->IsBound()) {
		return;
	}
	auto &bound_index = owned_index->Cast<BoundIndex>();
	if (!bound_index.SupportsDeltaIndexes()) {
		return;
	}

	auto constraint_type = bound_index.GetConstraintType();
	delete_indexes.AddIndex(bound_index.CreateEmptyCopy(constraint_type), index_oid, ConstraintCheckMode::DEFAULT);
	// Deferred constraints have no append index: transaction-local rows are verified when committing.
	if (append_indexes && !IsDeferred()) {
		append_indexes->AddIndex(bound_index.CreateEmptyCopy(constraint_type), index_oid, ConstraintCheckMode::DEFAULT);
	}
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
		if (result.Type() != ExceptionType::CONSTRAINT) {
			result.Throw();
		}
		throw InternalException("unexpected constraint violation on delete ART: ", result.Message());
	}
}

static void AppendIndexEntries(BoundIndex &index, DataChunk &chunk, Vector &row_ids) {
	IndexAppendInfo append_info;
	const auto error = index.Append(chunk, row_ids, append_info);
	if (error.HasError()) {
		if (error.Type() != ExceptionType::CONSTRAINT) {
			error.Throw();
		}
		throw InternalException("Failed to append to %s: %s", index.GetIndexName(), error.Message());
	}
}

//! Deletes the rows that are in the delta, and appends the others to the target
static void DeleteOrAppend(BoundIndex &delta, BoundIndex &target, DataChunk &chunk, Vector &row_ids) {
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	const auto delete_count = delta.TryDelete(chunk, row_ids, nullptr, remaining_sel);
	if (delete_count == chunk.size()) {
		return;
	}
	if (delete_count == 0) {
		AppendIndexEntries(target, chunk, row_ids);
		return;
	}
	// the delete covers rows from both before and after the delta was created
	const auto remaining_count = chunk.size() - delete_count;
	DataChunk remaining_chunk;
	remaining_chunk.InitializeEmpty(chunk.GetTypes());
	remaining_chunk.Slice(chunk, remaining_sel, remaining_count);
	Vector remaining_row_ids(row_ids, remaining_sel, remaining_count);
	AppendIndexEntries(target, remaining_chunk, remaining_row_ids);
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

static void ApplyIndexRemovalToDeltas(BoundIndex &index, IndexDeltas &deltas, DataChunk &chunk, Vector &row_ids,
                                      const IndexRemovalType removal_type) {
	D_ASSERT(removal_type != IndexRemovalType::DELETED_ROWS_IN_USE);

	switch (removal_type) {
	case IndexRemovalType::MAIN_INDEX_ONLY:
	case IndexRemovalType::MAIN_INDEX: {
		// Removing from the main index cannot happen directly during a checkpoint or a build; add the removal to a
		// delta index.
		auto &removed_data = deltas.GetOrCreate(index, IndexDeltaType::REMOVED_DATA);
		auto added_data = deltas.Find(IndexDeltaType::ADDED_DATA);

		// If we have also added data to the deltas, we might need to remove from there instead.
		// We FIRST try to remove from "added_data"; any rows that are not there are added to "removed_data".
		if (added_data) {
			DeleteOrAppend(*added_data, removed_data, chunk, row_ids);
		} else {
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
		auto &removed_data = deltas.GetOrCreate(index, IndexDeltaType::REMOVED_DATA);
		// Revert adding to the main index.
		// We have added data to the deltas as well, so the removal might have EITHER:
		// (1) added data to "removed_data" or
		// (2) removed data from "added_data".
		// Revert by first trying to remove from "removed_data"; any rows that were not removed are re-added to
		// "added_data".
		if (auto added_data = deltas.Find(IndexDeltaType::ADDED_DATA)) {
			DeleteOrAppend(removed_data, *added_data, chunk, row_ids);
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
	if (building) {
		if (build_error.HasError()) {
			return;
		}
		if (removal_type != IndexRemovalType::DELETED_ROWS_IN_USE) {
			build_changed_rows += chunk.size();
		}
		try {
			if (removal_type == IndexRemovalType::DELETED_ROWS_IN_USE) {
				ApplyIndexRemoval(bound_index, deltas, chunk, row_ids, removal_type);
			} else {
				ApplyIndexRemovalToDeltas(bound_index, deltas, chunk, row_ids, removal_type);
			}
		} catch (std::exception &ex) {
			RecordBuildError(ex);
		}
		return;
	}
	// Check which indexes we should append to or remove from. This method might also involve appending to indexes:
	// delta indexes must be filled with data we are removing, or we may be reverting a previous removal.
	// Not all indexes require delta indexes, so those skip "deleted_rows_in_use" bookkeeping.
	if (removal_type == IndexRemovalType::DELETED_ROWS_IN_USE) {
		// Cleanup always removes directly from "deleted_rows_in_use", even during a checkpoint.
		ApplyIndexRemoval(bound_index, deltas, chunk, row_ids, removal_type);
	} else if (bound_index.SupportsDeltaIndexes() && UseDeltas(active_checkpoint)) {
		// During a checkpoint, route changes through the deltas.
		ApplyIndexRemovalToDeltas(bound_index, deltas, chunk, row_ids, removal_type);
	} else {
		ApplyIndexRemoval(bound_index, deltas, chunk, row_ids, removal_type);
	}
}

bool IndexEntry::IsUnique() const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->IsUnique();
}

bool IndexEntry::IsDeferred() const {
	return check_mode == ConstraintCheckMode::DEFERRED;
}

bool IndexEntry::IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const ForeignKeyType fk_type) const {
	auto entry_lock = lock.GetSharedLock();
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE) {
		// Foreign keys cannot reference a deferred key.
		if (!owned_index->IsUnique() || IsDeferred()) {
			return false;
		}
	} else if (!owned_index->IsForeign()) {
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

Identifier IndexEntry::GetName() const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->GetIndexName();
}

string IndexEntry::GetIndexType() const {
	auto entry_lock = lock.GetSharedLock();
	return owned_index->GetIndexType();
}

bool IndexEntry::HasBufferedReplays() const {
	auto entry_lock = lock.GetSharedLock();
	if (!owned_index || owned_index->IsBound()) {
		return false;
	}
	return owned_index->Cast<UnboundIndex>().HasBufferedReplays();
}

void IndexEntry::Retire() {
	auto entry_lock = lock.GetExclusiveLock();
	deltas.Reset();
	owned_index->ResetStorage();
	owned_index.reset();
	bind_state = IndexBindState::RETIRED;
}

unique_ptr<BoundIndex> IndexEntry::Bind(IndexBinder &binder, const vector<LogicalType> &table_types,
                                        bool skip_mismatched_keys) {
	auto entry_lock = lock.GetExclusiveLock();
	return owned_index->Cast<UnboundIndex>().Bind(binder, table_types, skip_mismatched_keys);
}

void IndexEntry::CommitBind(unique_ptr<BoundIndex> bound_index) {
	auto entry_lock = lock.GetExclusiveLock();
	owned_index = std::move(bound_index);
	bind_state = IndexBindState::BOUND;
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
		index_append_info.delete_indexes.push_back(delete_entry->owned_index->Cast<BoundIndex>());
	}
	if (!manager) {
		if (auto delta = deltas.Find(IndexDeltaType::REMOVED_DATA)) {
			index_append_info.delete_indexes.push_back(*delta);
		}
	}
	bound_index.VerifyAppend(chunk, index_append_info, manager);
	if (auto delta = deltas.Find(IndexDeltaType::ADDED_DATA)) {
		// rows committed while a checkpoint runs are only in this delta
		delta->VerifyAppend(chunk, index_append_info, manager);
	}
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
		index_append_info.delete_indexes.push_back(delete_entry->owned_index->Cast<BoundIndex>());
	}
	if (auto delta = deltas.Find(IndexDeltaType::REMOVED_DATA)) {
		index_append_info.delete_indexes.push_back(*delta);
	}

	bound_index.VerifyConstraint(chunk, index_append_info, conflict_manager);
	if (auto delta = deltas.Find(IndexDeltaType::ADDED_DATA)) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_data_info;
		delta->VerifyConstraint(chunk, added_data_info, conflict_manager);
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
			if (error.Type() != ExceptionType::CONSTRAINT) {
				error.Throw();
			}
			throw InternalException("Failed to rebuild index %s after vacuum: %s", bound_index.GetIndexName(),
			                        error.Message());
		}
	};
	scan(bound_index.GetColumnIds(), append);
	bound_index.Verify();
}

void IndexEntry::RemapRowIds(const IndexRemapScan &scan) {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(owned_index->IsBound());
	auto &bound_index = owned_index->Cast<BoundIndex>();

	// Delete old rowids first to avoid same-key rowid collisions within the task.
	scan([&](DataChunk &chunk, Vector &old_row_ids, Vector &) { bound_index.Delete(chunk, old_row_ids); });
	// Remapping must not re-run uniqueness checks for already-validated rows.
	scan([&](DataChunk &chunk, Vector &, Vector &new_row_ids) {
		IndexAppendInfo append_info(IndexAppendMode::INSERT_DUPLICATES, nullptr);
		auto error = bound_index.Append(chunk, new_row_ids, append_info);
		if (error.HasError()) {
			error.Throw();
		}
	});
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
	result.check_mode = check_mode;
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

IndexCheckpointResult IndexEntry::Checkpoint(TableIndexWriter &index_writer) {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(owned_index);

	auto storage_version = index_writer.GetStorageVersion();
	if (owned_index->GetCheckpointMode() == IndexCheckpointMode::DEFERRED) {
		if (owned_index->IsBound() && !owned_index->Cast<BoundIndex>().SupportsDeltaIndexes()) {
			throw InternalException("Deferred index checkpointing requires delta index support");
		}
		return owned_index->Checkpoint(index_writer.GetSharedPartialBlockManager(), storage_version);
	}

	auto partial_block_manager = index_writer.CreatePerIndexPartialBlockManager();
	auto checkpoint = owned_index->Checkpoint(partial_block_manager, storage_version);
	partial_block_manager.FlushPartialBlocks(index_writer.GetContext());

	InstallShadowInternal(std::move(checkpoint.shadow_index));
	return checkpoint;
}

void IndexEntry::InstallShadow(unique_ptr<BoundIndex> shadow_index) {
	auto entry_lock = lock.GetExclusiveLock();
	InstallShadowInternal(std::move(shadow_index));
}

void IndexEntry::InstallShadowInternal(unique_ptr<BoundIndex> shadow_index) {
	if (!shadow_index) {
		return;
	}
	D_ASSERT(owned_index && owned_index->IsBound());
	owned_index = std::move(shadow_index);
}

IndexStorageInfo IndexEntry::SerializeToWAL(const StorageVersion version) {
	auto entry_lock = lock.GetExclusiveLock();
	// We never write an unbound index to the WAL.
	D_ASSERT(owned_index->IsBound());
	return owned_index->Cast<BoundIndex>().SerializeToWAL(version);
}

void IndexEntry::MergeCheckpointDeltas(const optional_idx checkpoint_id) {
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

void IndexEntry::MergeBuildDeltas(BoundIndex &index, const IndexBuildDeltas &deltas) {
	// remove first, so that a key moved to another row is not a duplicate
	for (const auto type : {IndexDeltaType::REMOVED_DATA, IndexDeltaType::ADDED_DATA}) {
		auto &delta = type == IndexDeltaType::REMOVED_DATA ? deltas.removed : deltas.added;
		if (!delta) {
			continue;
		}
		auto error = index.MergeDelta(type, *delta, IndexAppendMode::DEFAULT);
		if (error.HasError()) {
			if (error.Type() == ExceptionType::CONSTRAINT) {
				throw ConstraintException("Data contains duplicates on indexed column(s)");
			}
			error.Throw();
		}
	}
}

IndexBuildDeltas IndexEntry::TakeBuildDeltas() {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(building);
	if (build_error.HasError()) {
		build_error.Throw();
	}
	IndexBuildDeltas result;
	result.added = deltas.Take(IndexDeltaType::ADDED_DATA);
	result.removed = deltas.Take(IndexDeltaType::REMOVED_DATA);
	result.changed_rows = build_changed_rows;
	build_changed_rows = 0;
	return result;
}

void IndexEntry::FinishBuild(unique_ptr<BoundIndex> index, idx_t index_oid_p, ConstraintCheckMode check_mode_p) {
	auto entry_lock = lock.GetExclusiveLock();
	D_ASSERT(building);
	D_ASSERT(!build_error.HasError());
	D_ASSERT(!deltas.Find(IndexDeltaType::ADDED_DATA));
	D_ASSERT(!deltas.Find(IndexDeltaType::REMOVED_DATA));
	owned_index = std::move(index);
	index_oid = index_oid_p;
	check_mode = check_mode_p;
	building = false;
}

const unique_ptr<BoundIndex> &IndexDeltas::GetPointer(const IndexDeltaType type) const {
	switch (type) {
	case IndexDeltaType::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use;
	case IndexDeltaType::ADDED_DATA:
		return checkpoint.added_data;
	case IndexDeltaType::REMOVED_DATA:
		return checkpoint.removed_data;
	}
	throw InternalException("Unsupported index delta type");
}

unique_ptr<BoundIndex> &IndexDeltas::GetPointer(const IndexDeltaType type) {
	switch (type) {
	case IndexDeltaType::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use;
	case IndexDeltaType::ADDED_DATA:
		return checkpoint.added_data;
	case IndexDeltaType::REMOVED_DATA:
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
	for (const auto type : {IndexDeltaType::REMOVED_DATA, IndexDeltaType::ADDED_DATA}) {
		auto &delta = GetPointer(type);
		if (!delta) {
			continue;
		}
		// Inserts happen before deletes during commit, so duplicates must be accepted while merging the added rows.
		auto error = index.MergeDelta(type, *delta, IndexAppendMode::INSERT_DUPLICATES);
		if (error.HasError()) {
			return error;
		}
		delta.reset();
	}
	return ErrorData();
}

unique_ptr<BoundIndex> IndexDeltas::Take(const IndexDeltaType type) {
	return std::move(GetPointer(type));
}

void IndexDeltas::MarkWritten(const optional_idx checkpoint_id) {
	checkpoint.last_written_checkpoint = checkpoint_id;
}

void IndexDeltas::Reset() {
	deleted_rows_in_use.reset();
	checkpoint.added_data.reset();
	checkpoint.removed_data.reset();
	checkpoint.last_written_checkpoint = optional_idx();
}

} // namespace duckdb
