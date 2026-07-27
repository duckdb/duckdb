#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
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

IndexEntryReadGuard::IndexEntryReadGuard(IndexEntry &entry_p, unique_ptr<StorageLockKey> lock_p)
    : lock(std::move(lock_p)), entry(entry_p) {
}

Index &IndexEntryReadGuard::GetIndex() & {
	return *entry.owned_index;
}

const Index &IndexEntryReadGuard::GetIndex() const & {
	return *entry.owned_index;
}

optional_ptr<BoundIndex> IndexEntryReadGuard::DeletedRowsInUse() const {
	return entry.deleted_rows_in_use;
}

optional_ptr<BoundIndex> IndexEntryReadGuard::AddedDataDuringCheckpoint() const {
	return entry.added_data_during_checkpoint;
}

optional_ptr<BoundIndex> IndexEntryReadGuard::RemovedDataDuringCheckpoint() const {
	return entry.removed_data_during_checkpoint;
}

bool IndexEntryReadGuard::ShouldUseDeltaIndexes(const optional_idx active_checkpoint) const {
	if (!active_checkpoint.IsValid()) {
		return false;
	}
	if (!entry.last_written_checkpoint.IsValid()) {
		return true;
	}
	return active_checkpoint.GetIndex() != entry.last_written_checkpoint.GetIndex();
}

IndexEntryWriteGuard::IndexEntryWriteGuard(IndexEntry &entry_p, unique_ptr<StorageLockKey> lock_p)
    : IndexEntryReadGuard(entry_p, std::move(lock_p)) {
}

unique_ptr<BoundIndex> &IndexEntryWriteGuard::DeletedRowsInUse() {
	return entry.deleted_rows_in_use;
}

unique_ptr<BoundIndex> &IndexEntryWriteGuard::AddedDataDuringCheckpoint() {
	return entry.added_data_during_checkpoint;
}

unique_ptr<BoundIndex> &IndexEntryWriteGuard::RemovedDataDuringCheckpoint() {
	return entry.removed_data_during_checkpoint;
}

void IndexEntryWriteGuard::MarkWrittenForCheckpoint(const transaction_t checkpoint_id) {
	entry.last_written_checkpoint = checkpoint_id;
}

void IndexEntryWriteGuard::ReplaceIndex(unique_ptr<Index> index) {
	entry.owned_index = std::move(index);
}

template <class T>
TableIndexIterationHelper<T>::TableIndexIterationHelper(mutex &index_lock,
                                                        const vector<shared_ptr<IndexEntry>> &index_entries)
    : lock(index_lock), index_entries(index_entries) {
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

template <>
IndexEntry &TableIndexIterationHelper<IndexEntry>::TableIndexIterator::operator*() const {
	return *index_entries->at(index.GetIndex());
}

template <>
IndexEntryReadGuard TableIndexIterationHelper<IndexEntryReadGuard>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex())->ReadLock();
}

template <>
IndexEntryWriteGuard TableIndexIterationHelper<IndexEntryWriteGuard>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex())->WriteLock();
}

TableIndexIterationHelper<IndexEntry> TableIndexList::IndexEntries() const {
	return TableIndexIterationHelper<IndexEntry>(index_entries_lock, index_entries);
}

TableIndexIterationHelper<IndexEntryReadGuard> TableIndexList::ReadLockedIndexes() const {
	return TableIndexIterationHelper<IndexEntryReadGuard>(index_entries_lock, index_entries);
}

TableIndexIterationHelper<IndexEntryWriteGuard> TableIndexList::WriteLockedIndexes() const {
	return TableIndexIterationHelper<IndexEntryWriteGuard>(index_entries_lock, index_entries);
}

vector<shared_ptr<IndexEntry>> TableIndexList::GetEntries() const {
	lock_guard<mutex> lock(index_entries_lock);
	return index_entries;
}

template class TableIndexIterationHelper<IndexEntry>;
template class TableIndexIterationHelper<IndexEntryReadGuard>;
template class TableIndexIterationHelper<IndexEntryWriteGuard>;

void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(index_entries_lock);
	auto index_entry = make_shared_ptr<IndexEntry>(std::move(index));
	index_entries.push_back(std::move(index_entry));
	auto guard = index_entries.back()->ReadLock();
	if (!guard.GetIndex().IsBound()) {
		unbound_count++;
	}
}

void TableIndexList::RemoveIndex(const Identifier &name) {
	lock_guard<mutex> lock(index_entries_lock);
	for (idx_t i = 0; i < index_entries.size(); i++) {
		auto guard = index_entries[i]->WriteLock();
		auto &index = guard.GetIndex();
		if (index.GetIndexName() == name) {
			if (!index.IsBound()) {
				unbound_count--;
			}
			index.ResetStorage();
			index_entries.erase_at(i);
			return;
		}
	}
}

unordered_set<string> TableIndexList::DistinctIndexTypes() const {
	unordered_set<string> result;
	for (auto guard : ReadLockedIndexes()) {
		result.insert(guard.GetIndex().GetIndexType());
	}
	return result;
}

bool TableIndexList::AllIndexesBoundOfType(const char *index_type) const {
	for (auto guard : ReadLockedIndexes()) {
		const auto &index = guard.GetIndex();
		if (!index.IsBound() || index.GetIndexType() != index_type) {
			return false;
		}
	}
	return true;
}

bool TableIndexList::NameIsUnique(const string &name) const {
	// Only covers PK, FK, and UNIQUE indexes.
	for (auto guard : ReadLockedIndexes()) {
		const auto &index = guard.GetIndex();
		if (index.IsPrimary() || index.IsForeign() || index.IsUnique()) {
			if (index.GetIndexName() == name) {
				return false;
			}
		}
	}
	return true;
}

shared_ptr<IndexEntry> TableIndexList::FindEntry(const Identifier &name) const {
	lock_guard<mutex> lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		auto guard = entry->ReadLock();
		const auto &index = guard.GetIndex();
		if (index.GetIndexName() != name) {
			continue;
		}
		if (!index.IsBound()) {
			throw InternalException("TableIndexList::Find cannot return an unbound index");
		}
		return entry;
	}
	return nullptr;
}

void TableIndexList::Bind(ClientContext &context, DataTableInfo &table_info, const char *index_type) {
	{
		// Early-out, if we have no unbound indexes.
		lock_guard<mutex> lock(index_entries_lock);
		if (unbound_count == 0) {
			return;
		}
	}

	// Get the table from the catalog, so we can add it to the binder.
	auto &catalog = table_info.GetDB().GetCatalog();
	auto schema = table_info.GetSchemaName();
	auto table_name = table_info.GetTableName();
	auto &table_entry =
	    catalog.GetEntry<TableCatalogEntry>(context, QualifiedName(catalog.GetName(), schema, table_name));
	auto &table = table_entry.Cast<DuckTableEntry>();

	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.emplace_back(col.Name());
	}

	unique_lock<mutex> lock(index_entries_lock);
	// Busy-spin trying to bind all indexes.
	while (true) {
		shared_ptr<IndexEntry> index_entry;
		for (auto &entry : index_entries) {
			auto guard = entry->ReadLock();
			const auto &index = guard.GetIndex();
			if (!index.IsBound() && (index_type == nullptr || index.GetIndexType() == index_type)) {
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
			auto guard = index_entry->ReadLock();
			auto &unbound_index = guard.GetIndex().Cast<UnboundIndex>();
			bound_idx = idx_binder.BindIndex(unbound_index);
			if (unbound_index.HasBufferedReplays()) {
				// For replaying buffered index operations, we only want the physical column types (skip over
				// generated column types).
				vector<LogicalType> physical_column_types;
				for (auto &col : table.GetColumns().Physical()) {
					physical_column_types.push_back(col.Type());
				}
				bound_idx->ApplyBufferedReplays(physical_column_types, unbound_index.GetBufferedReplays(),
				                                unbound_index.GetMappedColumnIds());
			}
		}

		// Commit the bound index to the index entry.
		lock.lock();
		auto current_entry = std::find(index_entries.begin(), index_entries.end(), index_entry);
		if (current_entry == index_entries.end()) {
			continue;
		}
		auto guard = index_entry->WriteLock();
		index_entry->SetBindState(IndexBindState::BOUND);
		guard.ReplaceIndex(std::move(bound_idx));
		unbound_count--;
	}
}

bool IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const Index &index, const ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index.IsUnique() : !index.IsForeign()) {
		return false;
	}
	if (fk_keys.size() != index.GetColumnIds().size()) {
		return false;
	}

	auto &column_ids = index.GetColumnIds();
	for (auto &fk_key : fk_keys) {
		bool found = false;
		for (auto &index_key : column_ids) {
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

shared_ptr<IndexEntry> TableIndexList::FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys,
                                                           const ForeignKeyType fk_type) {
	lock_guard<mutex> lock(index_entries_lock);
	for (auto &entry : index_entries) {
		auto guard = entry->ReadLock();
		const auto &index = guard.GetIndex();
		if (IsForeignKeyIndex(fk_keys, index, fk_type)) {
			return entry;
		}
	}
	return nullptr;
}

void TableIndexList::VerifyForeignKey(optional_ptr<LocalTableStorage> storage, const vector<PhysicalIndex> &fk_keys,
                                      DataChunk &chunk, ConflictManager &conflict_manager) {
	const auto fk_type = conflict_manager.GetVerifyExistenceType() == VerifyExistenceType::APPEND_FK
	                         ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE
	                         : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	// Check whether the chunk can be inserted in or deleted from the referenced table storage.
	auto entry = FindForeignKeyIndex(fk_keys, fk_type);
	if (!entry) {
		throw InternalException("TableIndexList::VerifyForeignKey failed to find foreign key index");
	}

	auto guard = entry->ReadLock();
	auto &index = guard.GetIndex();
	D_ASSERT(index.IsBound());
	IndexAppendInfo index_append_info;
	unique_ptr<IndexEntryReadGuard> delete_guard;
	if (storage) {
		auto delete_entry = storage->delete_indexes.FindEntry(index.GetIndexName());
		if (delete_entry) {
			delete_guard = make_uniq<IndexEntryReadGuard>(delete_entry->ReadLock());
			index_append_info.delete_indexes.push_back(delete_guard->GetIndex().Cast<BoundIndex>());
		}
	}
	if (guard.RemovedDataDuringCheckpoint()) {
		index_append_info.delete_indexes.push_back(*guard.RemovedDataDuringCheckpoint());
	}

	auto &main_index = index.Cast<BoundIndex>();
	main_index.VerifyConstraint(chunk, index_append_info, conflict_manager);
	if (guard.AddedDataDuringCheckpoint()) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_during_checkpoint_info;
		guard.AddedDataDuringCheckpoint()->VerifyConstraint(chunk, added_during_checkpoint_info, conflict_manager);
	}
}

unordered_set<column_t> TableIndexList::GetRequiredColumns() const {
	unordered_set<column_t> column_ids;
	for (auto guard : ReadLockedIndexes()) {
		const auto &index = guard.GetIndex();
		for (auto col_id : index.GetColumnIds()) {
			column_ids.insert(col_id);
		}
	}
	return column_ids;
}

IndexSerializationResult TableIndexList::SerializeToDisk(QueryContext context, const IndexSerializationInfo &info) {
	lock_guard<mutex> lock(index_entries_lock);

	IndexSerializationResult result;

	idx_t bound_count = 0;
	for (const auto &entry : index_entries) {
		auto guard = entry->ReadLock();
		if (guard.GetIndex().IsBound()) {
			bound_count++;
		}
	}
	result.bound_infos.reserve(bound_count);
	for (const auto &entry : index_entries) {
		auto guard = entry->ReadLock();
		auto &index = guard.GetIndex();
		if (!index.IsBound()) {
			// Unbound: reference existing storage info
			auto &unbound_index = index.Cast<UnboundIndex>();
			D_ASSERT(!unbound_index.GetStorageInfo().name.empty());
			result.ordered_infos.push_back(unbound_index.GetStorageInfo());
			continue;
		}
		// Bound: move new storage info into bound_infos, then reference it
		auto &bound_index = index.Cast<BoundIndex>();
		auto storage_info = bound_index.SerializeToDisk(context, info.options);
		D_ASSERT(storage_info.IsValid() && !storage_info.name.empty());
		result.bound_infos.push_back(std::move(storage_info));
		result.ordered_infos.push_back(result.bound_infos.back());
	}

	return result;
}

void TableIndexList::MergeCheckpointDeltas(transaction_t checkpoint_id) const {
	for (auto guard : WriteLockedIndexes()) {
		// Merge any data appended to the index while the checkpoint was running.
		auto &index = guard.GetIndex();
		if (!index.IsBound()) {
			continue;
		}
		auto &bound_index = index.Cast<BoundIndex>();
		if (guard.RemovedDataDuringCheckpoint() || guard.AddedDataDuringCheckpoint()) {
			if (bound_index.GetIndexType() != ART::TYPE_NAME) {
				throw InternalException("Concurrent changes made to a non-ART index");
			}

			auto &art = bound_index.Cast<ART>();

			if (guard.RemovedDataDuringCheckpoint()) {
				art.RemovalMerge(*guard.RemovedDataDuringCheckpoint());
			}
			if (guard.AddedDataDuringCheckpoint()) {
				// NOTE: we insert duplicates here (IndexAppendMode::INSERT_DUPLICATES)
				// this is necessary due to the way that data is inserted into indexes during transaction commit
				// essentially we always FIRST insert data into the index, THEN remove data
				// even if the data was logically removed first
				// i.e. if we have a transaction like: DELETE FROM tbl WHERE i=42; INSERT INTO tbl VALUES (42);
				// we will FIRST insert 42, THEN delete 42 from the index
				// We plan to change this in the future - see https://github.com/duckdblabs/duckdb-internal/issues/6886
				auto error = art.InsertMerge(*guard.AddedDataDuringCheckpoint(), IndexAppendMode::INSERT_DUPLICATES);
				if (error.HasError()) {
					throw InternalException("Failed to append while merging checkpoint deltas - this "
					                        "signifies a bug or broken index: %s",
					                        error.Message());
				}
			}
			guard.RemovedDataDuringCheckpoint().reset();
			guard.AddedDataDuringCheckpoint().reset();
		}
		guard.MarkWrittenForCheckpoint(checkpoint_id);
	}
}

void TableIndexList::InitializeIndexChunk(DataChunk &index_chunk, const vector<LogicalType> &table_types,
                                          vector<StorageIndex> &mapped_column_ids, DataTableInfo &data_table_info) {
	// table_chunk contains all table columns.
	// We only reference the index columns in the index chunk.
	auto &index_list = data_table_info.GetIndexes();
	auto indexed_columns = index_list.GetRequiredColumns();

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
