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

const BoundIndex &IndexEntry::GetDelta(const IndexEntryDelta delta) const {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		if (deleted_rows_in_use) {
			return *deleted_rows_in_use;
		}
		break;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		if (added_data_during_checkpoint) {
			return *added_data_during_checkpoint;
		}
		break;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		if (removed_data_during_checkpoint) {
			return *removed_data_during_checkpoint;
		}
		break;
	}
	throw InternalException("Attempted to access a missing index delta");
}

BoundIndex &IndexEntry::GetDelta(const IndexEntryDelta delta) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		if (deleted_rows_in_use) {
			return *deleted_rows_in_use;
		}
		break;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		if (added_data_during_checkpoint) {
			return *added_data_during_checkpoint;
		}
		break;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		if (removed_data_during_checkpoint) {
			return *removed_data_during_checkpoint;
		}
		break;
	}
	throw InternalException("Attempted to access a missing index delta");
}

bool IndexEntry::HasDelta(const IndexEntryDelta delta) const {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use != nullptr;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		return added_data_during_checkpoint != nullptr;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		return removed_data_during_checkpoint != nullptr;
	}
	throw InternalException("Unsupported index delta type");
}

bool IndexEntry::ShouldUseDeltaIndexes(const optional_idx active_checkpoint) const {
	if (!active_checkpoint.IsValid()) {
		return false;
	}
	if (!last_written_checkpoint.IsValid()) {
		return true;
	}
	return active_checkpoint.GetIndex() != last_written_checkpoint.GetIndex();
}

void IndexEntry::SetDelta(const IndexEntryDelta delta, unique_ptr<BoundIndex> index) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		deleted_rows_in_use = std::move(index);
		return;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		added_data_during_checkpoint = std::move(index);
		return;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		removed_data_during_checkpoint = std::move(index);
		return;
	}
	throw InternalException("Unsupported index delta type");
}

void IndexEntry::ResetDelta(const IndexEntryDelta delta) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		deleted_rows_in_use.reset();
		return;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		added_data_during_checkpoint.reset();
		return;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		removed_data_during_checkpoint.reset();
		return;
	}
	throw InternalException("Unsupported index delta type");
}

void IndexEntry::MergeRemovedDataDuringCheckpoint() {
	auto &art = owned_index->Cast<ART>();
	art.RemovalMerge(GetDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT));
}

ErrorData IndexEntry::MergeAddedDataDuringCheckpoint(const IndexAppendMode append_mode) {
	auto &art = owned_index->Cast<ART>();
	return art.InsertMerge(GetDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT), append_mode);
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
IndexHandle<Index> TableIndexIterationHelper<IndexHandle<Index>>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex())->GetHandle();
}

template <>
MutableIndexHandle<Index> TableIndexIterationHelper<MutableIndexHandle<Index>>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex())->GetMutableHandle();
}

TableIndexIterationHelper<IndexEntry> TableIndexList::IndexEntries() const {
	return TableIndexIterationHelper<IndexEntry>(index_entries_lock, index_entries);
}

TableIndexIterationHelper<IndexHandle<Index>> TableIndexList::IndexHandles() const {
	return TableIndexIterationHelper<IndexHandle<Index>>(index_entries_lock, index_entries);
}

TableIndexIterationHelper<MutableIndexHandle<Index>> TableIndexList::MutableIndexHandles() const {
	return TableIndexIterationHelper<MutableIndexHandle<Index>>(index_entries_lock, index_entries);
}

vector<shared_ptr<IndexEntry>> TableIndexList::GetEntries() const {
	lock_guard<mutex> lock(index_entries_lock);
	return index_entries;
}

template class TableIndexIterationHelper<IndexEntry>;
template class TableIndexIterationHelper<IndexHandle<Index>>;
template class TableIndexIterationHelper<MutableIndexHandle<Index>>;

void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(index_entries_lock);
	auto index_entry = make_shared_ptr<IndexEntry>(std::move(index));
	index_entries.push_back(std::move(index_entry));
	const auto index_handle = index_entries.back()->GetHandle();
	if (!index_handle->IsBound()) {
		unbound_count++;
	}
}

void TableIndexList::RemoveIndex(const Identifier &name) {
	lock_guard<mutex> lock(index_entries_lock);
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

shared_ptr<IndexEntry> TableIndexList::FindEntry(const Identifier &name) const {
	lock_guard<mutex> lock(index_entries_lock);
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
		lock_guard<mutex> lock(index_entries_lock);
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

	unique_lock<mutex> lock(index_entries_lock);
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

bool IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, const IndexHandle<Index> &index,
                       const ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index->IsUnique() : !index->IsForeign()) {
		return false;
	}
	auto column_ids = index->GetColumnIds();
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
		auto index = entry->GetHandle();
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

	auto bound_index = entry->GetMutableHandle<BoundIndex>();
	IndexAppendInfo index_append_info;
	unique_ptr<IndexHandle<BoundIndex>> delete_handle;
	if (storage) {
		auto delete_entry = storage->delete_indexes.FindEntry(bound_index->GetIndexName());
		if (delete_entry) {
			delete_handle = make_uniq<IndexHandle<BoundIndex>>(delete_entry->GetHandle<BoundIndex>());
			(*delete_handle)->AddToDeleteIndexes(index_append_info);
		}
	}
	if (bound_index.HasDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT)) {
		bound_index.GetDelta<BoundIndex>(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT)
		    .AddToDeleteIndexes(index_append_info);
	}

	bound_index->VerifyConstraint(chunk, index_append_info, conflict_manager);
	if (bound_index.HasDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_during_checkpoint_info;
		bound_index.GetDelta<BoundIndex>(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)
		    .VerifyConstraint(chunk, added_during_checkpoint_info, conflict_manager);
	}
}

unordered_set<column_t> TableIndexList::GetRequiredColumns() const {
	unordered_set<column_t> column_ids;
	for (auto index : IndexHandles()) {
		for (auto col_id : index->GetColumnIds()) {
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
		auto index = entry->GetMutableHandle();
		IndexStorageInfo storage_info;
		if (index->IsBound()) {
			auto bound_index = index.Into<BoundIndex>();
			storage_info = bound_index->SerializeToDisk(context, info.options);
		} else {
			auto unbound_index = index.Into<UnboundIndex>();
			storage_info = unbound_index->CopyStorageInfo();
		}
		D_ASSERT(storage_info.IsValid() && !storage_info.name.empty());
		result.owned_infos.push_back(std::move(storage_info));
		result.ordered_infos.push_back(result.owned_infos.back());
	}

	return result;
}

void TableIndexList::MergeCheckpointDeltas(transaction_t checkpoint_id) const {
	for (auto index : MutableIndexHandles()) {
		// Merge any data appended to the index while the checkpoint was running.
		if (!index->IsBound()) {
			continue;
		}
		if (index.HasDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT) ||
		    index.HasDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
			if (index->GetIndexType() != ART::TYPE_NAME) {
				throw InternalException("Concurrent changes made to a non-ART index");
			}

			if (index.HasDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT)) {
				index.MergeRemovedDataDuringCheckpoint();
			}
			if (index.HasDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
				// NOTE: we insert duplicates here (IndexAppendMode::INSERT_DUPLICATES)
				// this is necessary due to the way that data is inserted into indexes during transaction commit
				// essentially we always FIRST insert data into the index, THEN remove data
				// even if the data was logically removed first
				// i.e. if we have a transaction like: DELETE FROM tbl WHERE i=42; INSERT INTO tbl VALUES (42);
				// we will FIRST insert 42, THEN delete 42 from the index
				// We plan to change this in the future - see https://github.com/duckdblabs/duckdb-internal/issues/6886
				auto error = index.MergeAddedDataDuringCheckpoint(IndexAppendMode::INSERT_DUPLICATES);
				if (error.HasError()) {
					throw InternalException("Failed to append while merging checkpoint deltas - this "
					                        "signifies a bug or broken index: %s",
					                        error.Message());
				}
			}
			index.ResetDelta(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT);
			index.ResetDelta(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT);
		}
		index.MarkWrittenForCheckpoint(checkpoint_id);
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
