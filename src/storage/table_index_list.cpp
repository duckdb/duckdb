#include "duckdb/storage/table/table_index_list.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
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

const unique_ptr<BoundIndex> &IndexDeltas::GetPointer(const IndexEntryDelta delta) const {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		return checkpoint.added_data;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		return checkpoint.removed_data;
	}
	throw InternalException("Unsupported index delta type");
}

unique_ptr<BoundIndex> &IndexDeltas::GetPointer(const IndexEntryDelta delta) {
	switch (delta) {
	case IndexEntryDelta::DELETED_ROWS_IN_USE:
		return deleted_rows_in_use;
	case IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT:
		return checkpoint.added_data;
	case IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT:
		return checkpoint.removed_data;
	}
	throw InternalException("Unsupported index delta type");
}

optional_ptr<const BoundIndex> IndexDeltas::Get(const IndexEntryDelta delta) const {
	return GetPointer(delta).get();
}

optional_ptr<BoundIndex> IndexDeltas::Get(const IndexEntryDelta delta) {
	return GetPointer(delta).get();
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

void IndexDeltas::Set(const IndexEntryDelta delta, unique_ptr<BoundIndex> index) {
	D_ASSERT(index);
	D_ASSERT(!GetPointer(delta));
	GetPointer(delta) = std::move(index);
}

void IndexDeltas::Reset(const IndexEntryDelta delta) {
	GetPointer(delta).reset();
}

ErrorData IndexDeltas::MergeCheckpointDeltas(BoundIndex &index) {
	for (const auto delta : {&checkpoint.removed_data, &checkpoint.added_data}) {
		if (!*delta) {
			continue;
		}
		auto error = index.MergeCheckpointDelta(**delta);
		if (error.HasError()) {
			return error;
		}
		delta->reset();
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

TableIndexIterationHelper<MutableIndexHandle<Index>> TableIndexList::MutableIndexHandles() const {
	return TableIndexIterationHelper<MutableIndexHandle<Index>>(*this);
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
MutableIndexHandle<Index> TableIndexIterationHelper<MutableIndexHandle<Index>>::TableIndexIterator::operator*() const {
	return index_entries->at(index.GetIndex())->GetMutableHandle();
}

template class TableIndexIterationHelper<IndexHandle<Index>>;
template class TableIndexIterationHelper<MutableIndexHandle<Index>>;

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
	annotated_lock_guard<annotated_mutex> lock(index_entries_lock);
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
	if (auto delta = bound_index.GetDelta<BoundIndex>(IndexEntryDelta::REMOVED_DATA_DURING_CHECKPOINT)) {
		delta->AddToDeleteIndexes(index_append_info);
	}

	bound_index->VerifyConstraint(chunk, index_append_info, conflict_manager);
	if (auto delta = bound_index.GetDelta<BoundIndex>(IndexEntryDelta::ADDED_DATA_DURING_CHECKPOINT)) {
		// if we have added any rows during checkpoint - check in that index as well
		IndexAppendInfo added_during_checkpoint_info;
		delta->VerifyConstraint(chunk, added_during_checkpoint_info, conflict_manager);
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
	annotated_lock_guard<annotated_mutex> lock(index_entries_lock);

	IndexSerializationResult result;

	result.owned_infos.reserve(index_entries.size());
	for (const auto &entry : index_entries) {
		auto index = entry->GetMutableHandle();
		IndexStorageInfo storage_info;
		if (index->IsBound()) {
			auto bound_index = std::move(index).Into<BoundIndex>();
			storage_info = bound_index->SerializeToDisk(context, info.options);
		} else {
			auto unbound_index = std::move(index).Into<UnboundIndex>();
			storage_info = unbound_index->CopyStorageInfo();
		}
		D_ASSERT(storage_info.IsValid() && !storage_info.name.empty());
		result.owned_infos.push_back(std::move(storage_info));
		result.ordered_infos.push_back(result.owned_infos.back());
	}

	return result;
}

void TableIndexList::MergeCheckpointDeltas(const transaction_t checkpoint_id) const {
	for (auto index : MutableIndexHandles()) {
		// Merge any data appended to the index while the checkpoint was running.
		if (!index->IsBound()) {
			continue;
		}
		auto bound_index = std::move(index).Into<BoundIndex>();
		auto error = bound_index.MergeCheckpointDeltas();
		if (error.HasError()) {
			throw InternalException("Failed to merge checkpoint delta - this signifies a bug or broken index: %s",
			                        error.Message());
		}
		bound_index.MarkWrittenForCheckpoint(checkpoint_id);
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
