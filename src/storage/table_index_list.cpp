#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/storage/table/append_state.hpp"
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

IndexEntryReadGuard::IndexEntryReadGuard(shared_ptr<IndexEntry> entry_p, unique_ptr<StorageLockKey> lock_p)
    : entry(std::move(entry_p)), lock(std::move(lock_p)) {
}

const BoundIndex &IndexEntryReadGuard::GetDelta(const IndexEntryDelta delta) const {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		if (entry->deleted_rows_in_use) {
			return *entry->deleted_rows_in_use;
		}
		break;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		if (entry->added_data_during_checkpoint) {
			return *entry->added_data_during_checkpoint;
		}
		break;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		if (entry->removed_data_during_checkpoint) {
			return *entry->removed_data_during_checkpoint;
		}
		break;
	}
	throw InternalException("Attempted to access a missing index delta");
}

bool IndexEntryReadGuard::HasDelta(const IndexEntryDelta delta) const {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		return entry->deleted_rows_in_use != nullptr;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		return entry->added_data_during_checkpoint != nullptr;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		return entry->removed_data_during_checkpoint != nullptr;
	}
	throw InternalException("Unsupported index delta type");
}

bool IndexEntryReadGuard::ShouldUseDeltaIndexes(const optional_idx active_checkpoint) const {
	if (!active_checkpoint.IsValid()) {
		return false;
	}
	if (!entry->last_written_checkpoint.IsValid()) {
		return true;
	}
	return active_checkpoint.GetIndex() != entry->last_written_checkpoint.GetIndex();
}

IndexEntryWriteGuard::IndexEntryWriteGuard(shared_ptr<IndexEntry> entry_p, unique_ptr<StorageLockKey> lock_p)
    : IndexEntryReadGuard(std::move(entry_p), std::move(lock_p)) {
}

BoundIndex &IndexEntryWriteGuard::GetDelta(const IndexEntryDelta delta) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		if (entry->deleted_rows_in_use) {
			return *entry->deleted_rows_in_use;
		}
		break;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		if (entry->added_data_during_checkpoint) {
			return *entry->added_data_during_checkpoint;
		}
		break;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		if (entry->removed_data_during_checkpoint) {
			return *entry->removed_data_during_checkpoint;
		}
		break;
	}
	throw InternalException("Attempted to access a missing index delta");
}

void IndexEntryWriteGuard::SetDelta(const IndexEntryDelta delta, unique_ptr<BoundIndex> index) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		entry->deleted_rows_in_use = std::move(index);
		return;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		entry->added_data_during_checkpoint = std::move(index);
		return;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		entry->removed_data_during_checkpoint = std::move(index);
		return;
	}
	throw InternalException("Unsupported index delta type");
}

void IndexEntryWriteGuard::ResetDelta(const IndexEntryDelta delta) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		entry->deleted_rows_in_use.reset();
		return;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		entry->added_data_during_checkpoint.reset();
		return;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		entry->removed_data_during_checkpoint.reset();
		return;
	}
	throw InternalException("Unsupported index delta type");
}

void IndexEntryWriteGuard::MergeRemovedDataDuringCheckpoint() {
	auto &art = entry->owned_index->Cast<ART>();
	art.RemovalMerge(GetDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT));
}

ErrorData IndexEntryWriteGuard::MergeAddedDataDuringCheckpoint(const IndexAppendMode append_mode) {
	auto &art = entry->owned_index->Cast<ART>();
	return art.InsertMerge(GetDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT), append_mode);
}

void IndexEntryWriteGuard::MarkWrittenForCheckpoint(const transaction_t checkpoint_id) {
	entry->last_written_checkpoint = checkpoint_id;
}

void IndexEntryWriteGuard::ReplaceIndex(unique_ptr<Index> index) {
	entry->owned_index = std::move(index);
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
	if (!guard.Invoke(&Index::IsBound)) {
		unbound_count++;
	}
}

void TableIndexList::RemoveIndex(const Identifier &name) {
	lock_guard<mutex> lock(index_entries_lock);
	for (idx_t i = 0; i < index_entries.size(); i++) {
		auto guard = index_entries[i]->WriteLock();
		if (guard.Invoke(&Index::GetIndexName) == name) {
			if (!guard.Invoke(&Index::IsBound)) {
				unbound_count--;
			}
			guard.Invoke(&Index::ResetStorage);
			index_entries.erase_at(i);
			return;
		}
	}
}

unordered_set<string> TableIndexList::DistinctIndexTypes() const {
	unordered_set<string> result;
	for (auto guard : ReadLockedIndexes()) {
		result.insert(guard.Invoke(&Index::GetIndexType));
	}
	return result;
}

bool TableIndexList::AllIndexesBoundOfType(const char *index_type) const {
	for (auto guard : ReadLockedIndexes()) {
		if (!guard.Invoke(&Index::IsBound) || guard.Invoke(&Index::GetIndexType) != index_type) {
			return false;
		}
	}
	return true;
}

bool TableIndexList::NameIsUnique(const string &name) const {
	// Only covers PK, FK, and UNIQUE indexes.
	for (auto guard : ReadLockedIndexes()) {
		if (guard.Invoke(&Index::IsPrimary) || guard.Invoke(&Index::IsForeign) || guard.Invoke(&Index::IsUnique)) {
			if (guard.Invoke(&Index::GetIndexName) == name) {
				return false;
			}
		}
	}
	return true;
}

shared_ptr<IndexEntry> TableIndexList::FindEntry(const Identifier &name) const {
	lock_guard<mutex> lock(index_entries_lock);
	for (const auto &entry : index_entries) {
		if (entry->Read<Index>([name](const Index &index) { return index.GetIndexName() != name; })) {
			continue;
		};
		if (entry->Read<Index>([name](const Index &index) { return index.IsBound(); })) {
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
			if (!guard.Invoke(&Index::IsBound) &&
			    (index_type == nullptr || guard.Invoke(&Index::GetIndexType) == index_type)) {
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
			auto guard = index_entry->WriteLock();
			vector<LogicalType> physical_column_types;
			for (auto &col : table.GetColumns().Physical()) {
				physical_column_types.push_back(col.Type());
			}
			bound_idx = guard.Invoke(&UnboundIndex::Bind, idx_binder, physical_column_types);
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

bool IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const IndexEntryReadGuard &guard,
                       const ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !guard.Invoke(&Index::IsUnique)
	                                                         : !guard.Invoke(&Index::IsForeign)) {
		return false;
	}
	auto column_ids = guard.Invoke(&Index::GetColumnIds);
	if (fk_keys.size() != column_ids.size()) {
		return false;
	}

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
		if (IsForeignKeyIndex(fk_keys, guard, fk_type)) {
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

	auto guard = entry->WriteLock();
	D_ASSERT(guard.Invoke(&Index::IsBound));
	IndexAppendInfo index_append_info;
	unique_ptr<IndexEntryReadGuard> delete_guard;
	if (storage) {
		auto delete_entry = storage->delete_indexes.FindEntry(guard.Invoke(&Index::GetIndexName));
		if (delete_entry) {
			delete_guard = make_uniq<IndexEntryReadGuard>(delete_entry->ReadLock());
			delete_guard->Invoke(&BoundIndex::AddToDeleteIndexes, index_append_info);
		}
	}
	if (guard.HasDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT)) {
		guard.InvokeDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT, &BoundIndex::AddToDeleteIndexes,
		                  index_append_info);
	}

	guard.Invoke(&BoundIndex::VerifyConstraint, chunk, index_append_info, conflict_manager);
	if (guard.HasDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_during_checkpoint_info;
		guard.InvokeDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT, &BoundIndex::VerifyConstraint, chunk,
		                  added_during_checkpoint_info, conflict_manager);
	}
}

unordered_set<column_t> TableIndexList::GetRequiredColumns() const {
	unordered_set<column_t> column_ids;
	for (auto guard : ReadLockedIndexes()) {
		for (auto col_id : guard.Invoke(&Index::GetColumnIds)) {
			column_ids.insert(col_id);
		}
	}
	return column_ids;
}

IndexSerializationResult TableIndexList::SerializeToDisk(QueryContext context, const IndexSerializationInfo &info) {
	lock_guard<mutex> lock(index_entries_lock);

	IndexSerializationResult result;

	result.owned_infos.reserve(index_entries.size());
	for (const auto &entry : index_entries) {
		auto guard = entry->WriteLock();
		IndexStorageInfo storage_info;
		if (guard.Invoke(&Index::IsBound)) {
			storage_info = guard.Invoke(&BoundIndex::SerializeToDisk, context, info.options);
		} else {
			storage_info = guard.Invoke(&UnboundIndex::CopyStorageInfo);
		}
		D_ASSERT(storage_info.IsValid() && !storage_info.name.empty());
		result.owned_infos.push_back(std::move(storage_info));
		result.ordered_infos.push_back(result.owned_infos.back());
	}

	return result;
}

void TableIndexList::MergeCheckpointDeltas(transaction_t checkpoint_id) const {
	for (auto guard : WriteLockedIndexes()) {
		// Merge any data appended to the index while the checkpoint was running.
		if (!guard.Invoke(&Index::IsBound)) {
			continue;
		}
		if (guard.HasDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT) ||
		    guard.HasDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
			if (guard.Invoke(&Index::GetIndexType) != ART::TYPE_NAME) {
				throw InternalException("Concurrent changes made to a non-ART index");
			}

			if (guard.HasDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT)) {
				guard.MergeRemovedDataDuringCheckpoint();
			}
			if (guard.HasDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
				// NOTE: we insert duplicates here (IndexAppendMode::INSERT_DUPLICATES)
				// this is necessary due to the way that data is inserted into indexes during transaction commit
				// essentially we always FIRST insert data into the index, THEN remove data
				// even if the data was logically removed first
				// i.e. if we have a transaction like: DELETE FROM tbl WHERE i=42; INSERT INTO tbl VALUES (42);
				// we will FIRST insert 42, THEN delete 42 from the index
				// We plan to change this in the future - see https://github.com/duckdblabs/duckdb-internal/issues/6886
				auto error = guard.MergeAddedDataDuringCheckpoint(IndexAppendMode::INSERT_DUPLICATES);
				if (error.HasError()) {
					throw InternalException("Failed to append while merging checkpoint deltas - this "
					                        "signifies a bug or broken index: %s",
					                        error.Message());
				}
			}
			guard.ResetDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT);
			guard.ResetDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT);
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
